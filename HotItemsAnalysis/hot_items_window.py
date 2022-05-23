
import json
from typing import Any, Iterable

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, ProcessWindowFunction, TimeWindow
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.common import Duration, Time, Configuration
from pyflink.util.java_utils import get_j_env_configuration

import sys
sys.path.append('/home/hadoop/code/pyflink/Flink-UserBehaviorAnalysis')
from base_window import SlidingEventTimeWindows


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
                                [Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT()])
    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()
    kafka_consumer = FlinkKafkaConsumer(topics="user_behavior_2", deserialization_schema=json_row_schema,
                                        properties=properties)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))\
        .with_timestamp_assigner(KafkaRowTimestampAssigner())
    kafka_stream = env.add_source(kafka_consumer).assign_timestamps_and_watermarks(watermark_strategy)

    agg_stream = kafka_stream \
        .filter(lambda x: x['behavior'] == 'pv')\
        .key_by(lambda x: x['item_id']) \
        .window(SlidingEventTimeWindows.of(Time.seconds(100), Time.seconds(10)))
    agg_stream = agg_stream.process(CountWindowProcessFunction(), Types.TUPLE([Types.INT(), Types.LONG(), Types.LONG(), Types.INT()]))
    agg_stream.print()

    env.execute('hot items')

class KafkaRowTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[4] * 1000

class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, int, TimeWindow]):
    def process(self,
                key: int,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, context.window().start, context.window().end, len([e for e in elements]))]

    def clear(self, context: ProcessWindowFunction.Context) -> None:
        pass

if __name__ == '__main__':
    hot_items_stat()
