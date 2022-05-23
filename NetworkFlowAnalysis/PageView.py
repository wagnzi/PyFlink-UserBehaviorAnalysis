import random
from typing import Any, Iterable, Tuple

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Row, Types, Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.functions import MapFunction, AggregateFunction, WindowFunction, \
    KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from base_window import TumblingEventTimeWindows, TimeWindow
from window_utils import aggregate


def page_view():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    input_stream = env.read_text_file('hdfs://master:8020/data/UserBehavior.csv')

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2)).with_timestamp_assigner(row_timestamp_assiger())
    output_info = Types.ROW_NAMED(['user_id', 'item_id', 'category_id', 'behavior', 'timestamp'],
                                  [Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.LONG()])
    data_stream = input_stream.flat_map(convert_type, output_type=output_info).\
        assign_timestamps_and_watermarks(watermark_strategy)

    agg_stream = data_stream.filter(lambda x: x[3] == 'pv').map(MyMapper()).\
        key_by(lambda x: x[1]).window(TumblingEventTimeWindows.of(Time.seconds(30)))
    pv_stream = aggregate(agg_stream, PvCountAgg(), PvCountWindowResult(), accumulator_type=Types.LONG(), output_type=Types.TUPLE([Types.LONG(), Types.INT()]))

    total_pv_stream = pv_stream.key_by(lambda x: x[0]).process(TotalPvCountResult())
    total_pv_stream.print()

    env.execute("pv job")

def convert_type(row):
    line = row.split(',')
    user_id, item_id, category_id, behavior, timestamp = int(line[0]), int(line[1]), int(line[2]), line[3], int(line[4])
    yield Row(user_id, item_id, category_id, behavior, timestamp)

class row_timestamp_assiger(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[4] * 1000

class MyMapper(MapFunction):
    def map(self, value):
        return str(random.randint(0, 1000)), 1

class PvCountAgg(AggregateFunction):
    def create_accumulator(self):
        return 0
    def add(self, value, accumulator):
        return accumulator + 1
    def get_result(self, accumulator):
        return accumulator
    def merge(self, acc_a, acc_b):
        return acc_a + acc_b

class PvCountWindowResult(WindowFunction):
    def apply(self, key: int, window: TimeWindow, inputs: Iterable[int]) -> Iterable[Tuple]:
        window_end = window.end
        window_count = iter(inputs).__next__()
        return [(window_end, window_count)]

class TotalPvCountResult(KeyedProcessFunction):
    def __init__(self):
        self.total_pvcount_state = None

    def open(self, runtime_context: RuntimeContext):
        pvcount_state_descriptor = ValueStateDescriptor("total-pv", Types.LONG())
        self.total_pvcount_state = runtime_context.get_state(pvcount_state_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        current_total_count = self.total_pvcount_state.value()
        if current_total_count is None:
            current_total_count = 0
        self.total_pvcount_state.update(current_total_count + value[1])
        ctx.timer_service().register_event_time_timer(value[0] + 1)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        total_pvcount = self.total_pvcount_state.value()
        yield ctx.get_current_key(), total_pvcount
        self.total_pvcount_state.clear()



if __name__ == '__main__':
    page_view()
