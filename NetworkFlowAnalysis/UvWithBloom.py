from typing import Any, Iterable

from pyflink.common import Types, Time, Configuration
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.window import Trigger, TriggerResult
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext
from pyflink.util.java_utils import get_j_env_configuration

from base_window import TumblingEventTimeWindows, TimeWindow

import redis
import pickle

def unique_vistor():
    env = StreamExecutionEnvironment.get_execution_environment()
    config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
    config.set_integer("python.fn-execution.bundle.size", 10)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(1)

    watermark_stragery = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
        row_timestamp_assigner())
    input_stream = env.read_text_file('hdfs://master:8020/data/UserBehavior.csv').flat_map(convert_type,
                output_type=Types.TUPLE([Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.FLOAT()])).\
                assign_timestamps_and_watermarks(watermark_stragery)

    uv_stream = input_stream.filter(lambda x: x[3] == 'pv').\
                map(lambda x: ("uv", x[0])).\
                key_by(lambda x: x[1]).\
                window(TumblingEventTimeWindows.of(Time.hours(1))).\
                trigger(MyTrigger()).\
                process(UvCountWithBloom())
    uv_stream.print()

    env.execute("uv with bloom job")


def convert_type(row):
    line = row.split(',')
    user_id, item_id, category_id, behavior, timestamp = int(line[0]), int(line[1]), int(line[2]), line[3], float(line[4])
    yield user_id, item_id, category_id, behavior, timestamp

class row_timestamp_assigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return value[4] * 1000

class MyTrigger(Trigger):
    def on_element(self, element: tuple, timestamp: int,
                   window: TimeWindow, ctx: 'Trigger.TriggerContext') -> TriggerResult:
        return TriggerResult.FIRE_AND_PURGE

    def on_processing_time(self, time: int, window: TimeWindow,
                           ctx: 'Trigger.TriggerContext') -> TriggerResult:
        return TriggerResult.CONTINUE

    def clear(self, window: TimeWindow, ctx: 'Trigger.TriggerContext') -> None:
        return None

    def on_event_time(self, time: int, window: TimeWindow, ctx: 'Trigger.TriggerContext') -> TriggerResult:
        return TriggerResult.CONTINUE

    def on_merge(self, window: TimeWindow, ctx: 'Trigger.OnMergeContext') -> None:
        pass


class UvCountWithBloom(ProcessWindowFunction):
    def __init__(self):
        self.jedis = None
        self.bloom_filter = None

    def open(self, runtime_context: RuntimeContext):
        pool = redis.ConnectionPool(decode_responses=True, host="192.168.200.100", port=6379,
                                    password="147258", db=0)
        self.jedis = redis.Redis(connection_pool=pool)
        self.bloom_filter = Bloom(1 << 29)

    def process(self, key: str, content: 'ProcessWindowFunction.Context', elements: Iterable[tuple]) -> Iterable[tuple]:
        stored_bitmap_key = str(content.window().end)

        uv_count_map = "uvcount"
        current_key = stored_bitmap_key
        count = 0

        temp = self.jedis.hget(uv_count_map, current_key)
        if temp is not None:
            count = float(temp)

        user_id = str(elements.__iter__().__next__()[1])
        offset = self.bloom_filter.hash(user_id, 61)
        is_exist = self.jedis.getbit(stored_bitmap_key, offset)
        if is_exist == 0:
            self.jedis.setbit(stored_bitmap_key, offset, True)
            self.jedis.hset(uv_count_map, current_key, str(count+1))
            yield float(current_key), count+1

    def clear(self, context: 'ProcessWindowFunction.Context') -> None:
        pass


class Bloom(object):
    def __init__(self, size):
        self.cap = size

    def hash(self, value, seed):
        result = 0
        for i in range(0, len(value)):
            result = result * seed + int(value[i])
        return (self.cap - 1) & result


if __name__ == '__main__':
    unique_vistor()
