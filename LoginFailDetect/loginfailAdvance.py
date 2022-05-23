import time
from typing import Any

from pyflink.common import Row, Types, Duration
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, KeyedProcessFunction, \
    RuntimeContext
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor


def split_input(row):
    line = row.split(',')
    user_id, ip, event_type, timestamp = int(line[0]), line[1], line[2], int(line[3])
    yield user_id, ip, event_type, timestamp


def login_fail_warning_advance():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    input_stream = env.read_text_file('LoginLog.csv')
    type_info =  Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()])
    login_event_stream = input_stream.flat_map(split_input, output_type=type_info)

    watermark_stragery = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3)).with_timestamp_assigner(row_timestamp_assigner())

    # 进行判断和检测，如果2秒内连续登录失败，输出报警信息
    login_fail_event_stream = login_event_stream.assign_timestamps_and_watermarks(watermark_stragery)\
                                                .key_by(lambda x: x[0]) \
                                                .process(login_fail_warning_advance_result())
    login_fail_event_stream.print("fail")
    env.execute("login fail detect job")

class row_timestamp_assigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[3] * 1000

class login_fail_warning_advance_result(KeyedProcessFunction):
    def __init__(self):
        self.login_fail_list_state = None

    def open(self, runtime_context: RuntimeContext):
        list_state = ListStateDescriptor('login-fail-list', Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()]))
        self.login_fail_list_state = runtime_context.get_list_state(list_state)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 判断事件类型
        if value[2] == 'fail':
            iter_info = iter(self.login_fail_list_state.get())

            # 转换成list，判断list长度是否为空，来判断list-state是否为空
            # iter_list = [i for i in iter_info]
            # first_fail_event = iter_list[0]

            # 设置迭代结束为None，来判断iter迭代器是否有元素
            first_fail_event = next(iter_info, None)
            # 判断是否有登录失败事件:
            if first_fail_event is not None:
                if value[3] < first_fail_event[3] + 2:
                    first_fail_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(first_fail_event[3]))
                    last_fail_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value[3]))

                    result = '用户ID = ' + str(first_fail_event[0]) + '\t' + \
                             '第一次失败时间: \t' + first_fail_time + '\t' + \
                             '最后一次失败时间: \t' + last_fail_time + '\t' + \
                             'login fail 2 times in 2s \t' + '\n' + \
                             '\n=======================================\n\n'
                    yield result
                # 不管是否报警，当前已经处理完，将状态更新为最近依次登录失败的事件
                self.login_fail_list_state.clear()
                self.login_fail_list_state.add(value)
            else:
                # 如果没有，将当前事件添加进list-state
                self.login_fail_list_state.add(value)
        else:
            # 如果成功，状态清空
            self.login_fail_list_state.clear()


if __name__ == '__main__':
    login_fail_warning_advance()

