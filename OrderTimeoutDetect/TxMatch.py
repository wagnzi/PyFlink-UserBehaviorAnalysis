from typing import Any

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.functions import KeyedCoProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


def tx_match():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(1)

    order_watermark_stragery = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(rowtime_order_assigner())
    order_event_stream = env.read_text_file('').\
        flat_map(convert_order, output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.FLOAT()])).\
        assign_timestamps_and_watermarks(order_watermark_stragery).filter(lambda x: x[2] != '')

    receipt_watermark_stragery = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(rowtime_receipt_assigner())
    receipt_event_stream = env.read_text_file("ReceiptLog.csv").\
        flat_map(convert_receipt, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])).\
        assign_timestamps_and_watermarks(receipt_watermark_stragery)

    # connect 类型一样，union必须类型一样，可多条流
    result_stream = order_event_stream.connect(receipt_event_stream).\
        key_by(lambda x: x[2], lambda x: x[0]).process(TxMatchDetect())
    result_stream.print("joined")

    env.execute("tx match join")

def convert_order(row):
    line = row.split(',')
    order_id, event_type, tx_id, timestamp = int(line[0]), line[1], line[2], float(line[3])
    yield order_id, event_type, tx_id, timestamp

def convert_receipt(row):
    line = row.split(',')
    tx_id, pay_channel, timestamp = line[0], line[1], line[2]
    yield tx_id, pay_channel, timestamp

class rowtime_order_assigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[3] * 1000

class rowtime_receipt_assigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[2] * 1000

class TxMatchDetect(KeyedCoProcessFunction):
    def __init__(self):
        self.pay_event_state = None
        self.receipt_event_state = None

    def open(self, runtime_context: RuntimeContext):
        pay_event_descriptor = ValueStateDescriptor("pay", Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.FLOAT()]))
        self.pay_event_state = runtime_context.get_state(pay_event_descriptor)

        receipt_event_descriptor = ValueStateDescriptor("receipt", Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))
        self.receipt_event_state = runtime_context.get_state(receipt_event_descriptor)

    def process_element1(self, value, ctx: 'KeyedCoProcessFunction.Context'):
        receipt = self.receipt_event_state.value()
        if receipt is not None:
            yield value, receipt
            self.pay_event_state.clear()
            self.receipt_event_state.clear()
        else:
            # 注册定时器，等待receipt
            self.pay_event_state.update(value)
            ctx.timer_service().register_event_time_timer(value[3] * 1000 + 5000)

    def process_element2(self, value, ctx: 'KeyedCoProcessFunction.Context'):
        pay = self.pay_event_state.value()
        if pay is not None:
            yield pay, value
            self.pay_event_state.clear()
            self.receipt_event_state.clear()
        else:
            # 注册定时器，等待pay
            self.receipt_event_state.update(value)
            ctx.timer_service().register_event_time_timer(value[2] * 1000 + 5000)

    def on_timer(self, timestamp: int, ctx: 'KeyedCoProcessFunction.OnTimerContext'):
        if self.pay_event_state.value() is not None:
            # 侧边输出只有pay，没有receipt
            pass
        if self.receipt_event_state.value() is not None:
            # 侧边输出只有receipt，没有pay
            pass





if __name__ == '__main__':
    tx_match()
