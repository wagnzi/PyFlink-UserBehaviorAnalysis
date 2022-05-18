import time
from typing import Any

from pyflink.common import Row, Types, Duration, Configuration
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, KeyedProcessFunction, \
    RuntimeContext
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.util.java_utils import get_j_env_configuration


def split_input(row):
    line = row.split(',')
    user_id, ip, event_type, timestamp = int(line[0]), line[1], line[2], float(line[3])
    yield user_id, ip, event_type, timestamp


def login_fail_warning():
    env = StreamExecutionEnvironment.get_execution_environment()
    config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
    config.set_integer("python.fn-execution.bundle.size", 10)
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    input_stream = env.read_text_file('LoginLog.csv')
    type_info =  Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()])
    login_event_stream = input_stream.flat_map(split_input, output_type=type_info)

    watermark_stragery = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3)).with_timestamp_assigner(row_timestamp_assigner())

    # 进行判断和检测，如果2秒内连续登录失败，输出报警信息
    login_fail_event_stream = login_event_stream.assign_timestamps_and_watermarks(watermark_stragery)\
                                                .key_by(lambda x: x[0]) \
                                                .process(login_fail_warning_result(2))
    login_fail_event_stream.print("fail")
    env.execute("login fail detect job")

class row_timestamp_assigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[3] * 1000

class login_fail_warning_result(KeyedProcessFunction):

    def __init__(self, fail_times):
        self.fail_time = fail_times
        self.login_fail_list_state = None
        self.timer_ts_state = None

    def open(self, runtime_context: RuntimeContext):
        # 定义状态，保存当前所有的登录失败事件，保存定时器事件戳
        list_state_descriptor = ListStateDescriptor("login_fail_list", Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()]))
        self.login_fail_list_state = runtime_context.get_list_state(list_state_descriptor)

        ts_value_descriptor = ValueStateDescriptor("timer_ts", Types.LONG())
        self.timer_ts_state = runtime_context.get_state(ts_value_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 判断当前登录事件是成功还是失败
        if value[2] == 'fail':
            self.login_fail_list_state.add(value)
            # 如果没有定时器，注册一个2秒后的定时器
            if self.timer_ts_state.value() is None:
                ts = value[3] * 1000 + 2000
                ctx.timer_service().register_event_time_timer(ts)
                self.timer_ts_state.update(ts)
        else:
            # 如果是成功，则清空状态和定时器，重新开始
            if self.timer_ts_state.value() is not None:
                ctx.timer_service().delete_event_time_timer(self.timer_ts_state.value())
            self.login_fail_list_state.clear()
            self.timer_ts_state.clear()

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        all_login_fail_list = list()
        iter_info = iter(self.login_fail_list_state.get())
        for item in iter_info:
            all_login_fail_list.append(item)

        # 判断登录失败事件的个数，如果超过上限，报警
        login_fail_times = len(all_login_fail_list)
        if login_fail_times >= self.fail_time:
            head = all_login_fail_list[0]
            last = all_login_fail_list[login_fail_times-1]

            first_fail_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(head[3]))
            last_fail_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last[3]))

            result = '用户ID = ' + str(head[0]) + '\t' + \
                     '第一次失败时间: \t' + first_fail_time + '\t' + \
                    '最后一次失败时间: \t' + last_fail_time + '\t' + \
                    '2s内登录失败次数: \t' + str(login_fail_times) + '\n' + \
                     '\n=======================================\n\n'
            time.sleep(1)
            yield result

        self.login_fail_list_state.clear()
        self.timer_ts_state.clear()




if __name__ == '__main__':
    login_fail_warning()
