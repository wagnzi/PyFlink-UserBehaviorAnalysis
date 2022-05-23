from typing import Any

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

def orderpay_timeout_without_cep():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(1)

    input_stream = env.read_text_file('OrderLog.csv').flat_map(convert_type,
                                                               output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()]))
    watermark_stragery = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(rowtime_assigner())
    order_event_stream = input_stream.assign_timestamps_and_watermarks(watermark_stragery)

    order_result_stream = order_event_stream.key_by(lambda x: x[0]).process(OrderPayDetect(), output_type=Types.TUPLE([Types.INT(), Types.STRING()]))
    order_result_stream.print()

    env.execute("order pay timeout without cep job")

def convert_type(row):
    line = row.split(',')
    order_id, event_type, tx_id, timestamp = int(line[0]), line[1], line[2], int(line[3])
    yield order_id, event_type, tx_id, timestamp

class rowtime_assigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[3] * 1000

class OrderPayDetect(KeyedProcessFunction):
    def __init__(self):
        self.is_payed_state = None
        self.is_created_state = None
        self.timerts_state = None

    def open(self, runtime_context: RuntimeContext):
        is_payed_descriptor = ValueStateDescriptor("is-payed", Types.BOOLEAN())
        self.is_payed_state = runtime_context.get_state(is_payed_descriptor)

        is_created_descriptor = ValueStateDescriptor("is-created", Types.BOOLEAN())
        self.is_created_state = runtime_context.get_state(is_created_descriptor)

        timerts_descriptor = ValueStateDescriptor("timer-ts", Types.LONG())
        self.timerts_state = runtime_context.get_state(timerts_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        is_payed = self.is_payed_state.value()
        if is_payed is None:
            is_payed = False

        is_created = self.is_created_state.value()
        if is_created is None:
            is_created = False

        timerts = self.timerts_state.value()
        if timerts is None:
            timerts = 0

        if value[1] == 'create':
            if is_payed:
                # 如果已经支付过，匹配成功，输出到主流
                yield value[0], "payed successfully"
                # 清空定时器，清空状态
                ctx.timer_service().delete_event_time_timer(timerts)
                self.is_payed_state.clear()
                self.is_created_state.clear()
                self.timerts_state.clear()
            else:
                # 如果pay没来过，最正常情况，注册15分钟后的定时器，开始等待
                ts = value[3] * 1000 + 15 * 60 * 1000
                ctx.timer_service().register_event_time_timer(ts)
                self.timerts_state.update(ts)
                self.is_created_state(True)
        elif value[1] == 'pay':
            # 判断是否create过
            if is_created:
                # 如果已经create，匹配成功，确认pay时间是否超过定时器时间
                if value[3] * 1000 < timerts:
                    # 如果超时，正常输出到主流
                    yield value[0], "payed successfully"
                else:
                    # 已经超时，因为乱序数据到来，定时器没触发，输出到侧输出流
                    pass
                # 已经处理完当前订单状态，清空定时器
                ctx.timer_service().delete_event_time_timer(timerts)
                self.is_created_state.clear()
                self.is_payed_state.clear()
                self.timerts_state.clear()
            else:
                # 如果没有created过，乱序，注册定时器等待create
                ctx.timer_service().register_event_time_timer(value[3] * 1000)
                self.timerts_state.update(value[3] * 1000)
                self.is_payed_state.update(True)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        # 定时器触发，说明create和pay有一个没来
        if self.is_payed_state.value():
            # 如果pay过，说明create没来，结果输出到
            pass
        else:
            # 如果pay过，真正超时
            pass
        self.is_payed_state.clear()
        self.is_payed_state.clear()
        self.timerts_state.clear()



if __name__ == '__main__':
    orderpay_timeout_without_cep()
