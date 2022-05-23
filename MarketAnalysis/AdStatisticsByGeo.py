import time
from typing import Any, Iterable, Tuple

from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Configuration, WatermarkStrategy, Duration, Row, Types, Time
from pyflink.util.java_utils import get_j_env_configuration
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, AggregateFunction, WindowFunction
from pyflink.datastream.state import ValueStateDescriptor

import sys
sys.path.append('/home/hadoop/code/pyflink/Flink-UserBehaviorAnalysis')
from base_window import SlidingEventTimeWindows, TimeWindow
from window_utils import aggregate


def ad_click_analysis():
    env = StreamExecutionEnvironment.get_execution_environment()
    config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
    config.set_integer('python.fn-execution.bundle.size', 10)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(1)

    input_stream = env.read_text_file('AdClickLog.csv')
    watermark_stragery = WatermarkStrategy.for_monotonous_timestamps().\
        with_timestamp_assigner(RowTimestampAssigner())
    output_info = Types.ROW_NAMED(['user_id', 'ad_id', 'province', 'city', 'timestamp'],
                                  Types.TUPLE([Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()]))
    adlog_stream = input_stream.flat_map(convert_type, output_type=output_info).\
        assign_timestamps_and_watermarks(watermark_stragery)

    # 过滤操作， 将有刷单行为的用户输出到侧输出流(黑名单报警)
    filter_blcaklist_user_stream = adlog_stream.key_by(lambda x: (x['user_id'], x['ad_id'])).process(FilterBlackListUserResult(100))

    # 根据省份做分组，开窗聚合统计
    adcount_window_stream = filter_blcaklist_user_stream.key_by(lambda x: x['province']).\
        window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(1)))
    agg_type_info = Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT()])
    adcount_result_stream = aggregate(adcount_window_stream, AdCountAgg(), AdCountWindowResult(),
                                      accumulator_type=Types.LONG(),
                                      output_type=agg_type_info
                                      )

    adcount_result_stream.print("count result")


class AdCountAgg(AggregateFunction):
    def create_accumulator(self):
        return 0

    def add(self, value, accumulator):
        return accumulator + 1

    def get_result(self, accumulator):
        return accumulator

    def merge(self, acc_a, acc_b):
        return acc_a + acc_b

class AdCountWindowResult(WindowFunction):
    def apply(self, key: str, window: TimeWindow, inputs: Iterable[int]) -> Iterable[Tuple]:
        end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(window.end))
        count = next(iter(inputs))
        return [(end, key, count)]

def convert_type(row):
    line = row.split(',')
    user_id, ad_id, province, city, timestamp = int(line[0]), int(line[1]), line[2], line[3], int(line[4])
    yield Row(user_id, ad_id, province, city, timestamp)

class RowTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[4] * 1000

class FilterBlackListUserResult(KeyedProcessFunction):

    def __init__(self, max_count):
        self.max_count = max_count
        self.count_state = None
        self.reset_timer_ts_state = None
        self.is_black_state = None

    def open(self, runtime_context: RuntimeContext):
        # 定义状态，保存用户对广告的点击量，每天0点定时清空状态的事件戳，标记当前用户是否已经进入黑名单
        count_state_descriptor = ValueStateDescriptor("count", Types.LONG())
        self.count_state = runtime_context.get_state(count_state_descriptor)

        reset_timer_state_descriptor = ValueStateDescriptor("reset-ts", Types.LONG())
        self.reset_timer_ts_state = runtime_context.get_state(reset_timer_state_descriptor)

        black_state_descriptor = ValueStateDescriptor("is-black", Types.BOOLEAN())
        self.is_black_state = runtime_context.get_state(black_state_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        cur_count = self.count_state.value()

        # 判断只要是第一个数据来了，直接注册0点的清空状态定时器
        if cur_count == 0:
            ts = (ctx.timer_service().current_processing_time() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000
            self.reset_timer_ts_state.update(ts)
            ctx.timer_service().register_event_time_timer(ts)

        # 判断count值是否已经到达定义的阈值，如果超过就输出到黑名单
        if cur_count >= self.max_count:
            # 判断是否已经在黑名单中，没有的话，输出到侧输出流
            if self.is_black_state.value() is None:
                self.is_black_state.update(True)
                # 侧边输出
            return

        # 正常情况，count加1，然后将数据原样输出
        self.count_state.update(cur_count + 1)
        yield value

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        if timestamp == self.reset_timer_ts_state.value():
            self.is_black_state.clear()
            self.count_state.clear()





if __name__ == '__main__':
    ad_click_analysis()
