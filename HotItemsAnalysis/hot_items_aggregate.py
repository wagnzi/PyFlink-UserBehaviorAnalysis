import json
from typing import Any, Iterable, Tuple
import time

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.common import Duration, Time, Configuration
from pyflink.datastream.functions import AggregateFunction, WindowFunction, KeyedProcessFunction, \
    RuntimeContext
from pyflink.datastream.state import ListStateDescriptor
from pyflink.util.java_utils import get_j_env_configuration

import sys
sys.path.append('/home/hadoop/code/pyflink/Flink-UserBehaviorAnalysis')
from base_window import SlidingEventTimeWindows, TimeWindow
from window_utils import aggregate


def convert_type(line):
    line = json.loads(line)
    user_id, item_id, category_id, behavior, timestamp = int(line['user_id']), int(line['item_id']), int(line['category_id']), line['behavior'], int(line['timestamp'])
    yield user_id, item_id, category_id, behavior, timestamp

def hot_items_stat():
    env = StreamExecutionEnvironment.get_execution_environment()
    config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
    config.set_integer("python.fn-execution.bundle.size", 10)
    env.set_parallelism(1)
    env.add_jars('file:///opt/cloudera/parcels/FLINK/lib/flink/lib/flink-sql-connector-kafka_2.11-1.14.3.jar')
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    properties = {
        'bootstrap.servers': '192.168.200.100:9092,192.168.200.101:9092,192.168.200.102:9092',
        'auto.offset.reset': 'earliest'
    }
    type_info = Types.ROW_NAMED(['user_id', 'item_id', 'category_id', 'behavior', 'timestamp'],
                                [Types.INT(), Types.INT(), Types.INT(), Types.STRING(),Types.INT()])
    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()
    kafka_consumer = FlinkKafkaConsumer(topics="user_behavior_2", deserialization_schema=json_row_schema,
                                        properties=properties)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))\
        .with_timestamp_assigner(KafkaRowTimestampAssigner())
    kafka_stream = env.add_source(kafka_consumer).assign_timestamps_and_watermarks(watermark_strategy)

    agg_stream = kafka_stream \
        .filter(lambda x: x['behavior'] == 'pv')\
        .key_by(lambda x: x['item_id']) \
        .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
    agg_type_info = Types.TUPLE([Types.INT(), Types.LONG(), Types.INT()])
    agg_stream = aggregate(agg_stream, CountAgg(), ItemViewWindowResult(),
                           accumulator_type=Types.LONG(),
                           output_type=agg_type_info)

    result_stream = agg_stream.key_by(lambda x: x[1]) \
        .process(TopNHotItems(5))
    result_stream.print("r")

    env.execute('hot items')

class KafkaRowTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[4] * 1000

class CountAgg(AggregateFunction):
    def create_accumulator(self) -> int:
        return 0
    def add(self, value: Tuple[int, int, int, str, int], accumulator: int) -> int:
        return accumulator + 1
    def get_result(self, accumulator: int) -> int:
        return accumulator
    def merge(self, acc_a: int, acc_b: int):
        return acc_a + acc_b

class AvgTs(AggregateFunction):
    def create_accumulator(self) -> Tuple[int, int]:
        return 0, 0
    def add(self, value: Tuple[int, int, int, str, int], accumulator: Tuple[int, int]) -> Tuple[int, int]:
        return accumulator[0] + value[4], accumulator[1] + 1
    def get_result(self, accumulator: Tuple[int, int]) -> float:
        return accumulator[0] / accumulator[1]
    def merge(self, acc_a: Tuple[int, int], acc_b: Tuple[int, int]) -> Tuple[int, int]:
        return acc_a[0] + acc_b[0], acc_a[1] + acc_b[1]

class ItemViewWindowResult(WindowFunction[int, Tuple, int, TimeWindow]):
    def apply(self, key: int, window: TimeWindow, inputs: Iterable[int]) -> Iterable[Tuple]:
        item_id = key
        windowEnd = window.end
        count = inputs.__iter__().__next__()
        return [(item_id, windowEnd, count)]



class TopNHotItems(KeyedProcessFunction):
    def __init__(self, top_size):
        self.top_size = top_size
        self.item_count_list_state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ListStateDescriptor("itemViewCount-list", Types.TUPLE([Types.INT(), Types.LONG(), Types.INT()]))
        self.item_count_list_state = runtime_context.get_list_state(state_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        self.item_count_list_state.add(value)
        ctx.timer_service().register_event_time_timer(value[1] + 1)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        all_items_view_counts = list()
        iter_info = iter(self.item_count_list_state.get())
        for item in iter_info:
            all_items_view_counts.append(item)
        self.item_count_list_state.clear()
        all_items_view_counts.sort(key=lambda x: x[2], reverse=True)
        sort_item_view_counts = all_items_view_counts[:self.top_size]

        time_format = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp/1000 - 1))
        result = '窗口结束时间: ' + time_format + '\n'

        for i in range(0, len(sort_item_view_counts)):
            current_item_view_count = sort_item_view_counts[i]
            result = result + 'NO' + str(i+1) + ': \t' + \
                     "商品ID = " + str(current_item_view_count[0]) + \
                     '\t' + "热门度 = " + str(current_item_view_count[2]) + '\n'
        result = result + '\n=======================================\n\n'
        time.sleep(1)
        yield result

if __name__ == '__main__':
    hot_items_stat()
