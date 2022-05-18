import time
from typing import Any, Iterable, Tuple
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Types, Duration, Time, Configuration
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.functions import AggregateFunction, WindowFunction, KeyedProcessFunction, \
    RuntimeContext
from pyflink.datastream.state import MapStateDescriptor
from pyflink.util.java_utils import get_j_env_configuration

import sys
sys.path.append('/home/hadoop/code/pyflink/Flink-UserBehaviorAnalysis')
from base_window import SlidingEventTimeWindows, TimeWindow
from window_utils import aggregate

def network_flow():
    env = StreamExecutionEnvironment.get_execution_environment()
    config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
    config.set_integer("python.fn-execution.bundle.size", 10)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(1)

    watermark_stragery = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(rowtime_assigner())
    input_stream = env.read_text_file('apache.log').\
        flat_map(convert_log, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG(), Types.STRING(), Types.STRING()]))\
        .assign_timestamps_and_watermarks(watermark_stragery)

    window_stream = input_stream.filter(lambda x: x[3] == 'GET').key_by(lambda x: x[4]).\
        window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(5))).\
        allowed_lateness(20 * 1000)
    agg_stream = aggregate(window_stream, CountAgg(), WindowsResult(),
                           accumulator_type=Types.LONG(),
                           output_type=Types.TUPLE([Types.STRING(), Types.LONG(), Types.INT()]))

    result_stream = agg_stream.key_by(lambda x: x[1]).process(TopNHotUrls(5))
    result_stream.print("result ")

    env.execute("network flow job")

def convert_log(row):
    line = row.strip().split(' ')
    ip, user_id, event_time_temp, method, url = line[0].strip(), line[1].strip(), line[3].strip(), line[5].strip(), line[6].strip()

    event_time = float(datetime.strptime(event_time_temp, '%d/%m/%Y:%H:%M:%S').timestamp())

    yield ip, user_id, event_time, method, url

class rowtime_assigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[2] * 1000

class CountAgg(AggregateFunction):
    def create_accumulator(self):
        return 0

    def add(self, value, accumulator):
        return accumulator + 1

    def get_result(self, accumulator):
        return accumulator

    def merge(self, acc_a, acc_b):
        return acc_a + acc_b

class WindowsResult(WindowFunction):
    def apply(self, key: str, window: TimeWindow, inputs: Iterable[int]) -> Iterable[Tuple]:
        return [(key, window.end, inputs.__iter__().__next__())]

class TopNHotUrls(KeyedProcessFunction):
    def __init__(self, top_size):
        self.top_size = top_size
        self.url_state = None

    def open(self, runtime_context: RuntimeContext):
        url_descriptor = MapStateDescriptor("url-state", Types.STRING(), Types.LONG())
        self.url_state = runtime_context.get_map_state(url_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        self.url_state.put(value[0], value[2])
        ctx.timer_service().register_event_time_timer(value[1] + 1)

        # 额外再注册一个定时器，2s后关闭，这是窗口已经彻底关闭，不会有聚合结果输出，清空状态
        ctx.timer_service().register_event_time_timer(value[1] + 20*1000)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        all_url_views = list()
        iter_info = iter(self.url_state.items())
        for info in iter_info:
            all_url_views.append([info[0], info[1]])

        # 判断触发器触发时间，如果已经是窗口结束时间1分钟之后，则直接清空状态
        if timestamp == ctx.get_current_key() + 20*1000:
            self.url_state.clear()
            return
        all_url_views.sort(key=lambda x: x[1], reverse=True)
        sort_url_views = all_url_views[:self.top_size]

        time_format = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp/1000 - 1))
        result = '时间: ' + time_format + '\n'

        for i in range(0, len(sort_url_views)):
            current_url_view = sort_url_views[i]
            result = result + 'NO' + str(i + 1) + ': \t' + "URL=" +current_url_view[0] + '\t' + "访问量="+str(current_url_view[1]) + '\n'
        result = result + '\n=======================================\n\n'
        time.sleep(1)
        yield result



if __name__ == '__main__':
    network_flow()
