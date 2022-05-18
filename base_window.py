import math
from typing import Iterable, Collection

from pyflink.common import TypeSerializer
from pyflink.datastream import WindowAssigner
from pyflink.datastream.window import T, Trigger, TimeWindowSerializer, \
    TriggerResult, mod_inverse, Window

from pyflink.common.time import Time


MAX_LONG_VALUE = 0x7fffffffffffffff
MIN_LONG_VALUE = - MAX_LONG_VALUE - 1


class TimeWindow(Window):
    """
    Window that represents a time interval from start (inclusive) to end (exclusive).
    """

    def __init__(self, start: int, end: int):
        super(TimeWindow, self).__init__()
        self.start = start
        self.end = end

    def max_timestamp(self) -> int:
        return self.end - 1

    def intersects(self, other: 'TimeWindow') -> bool:
        """
        Returns True if this window intersects the given window.
        """
        return self.start <= other.end and self.end >= other.start

    def cover(self, other: 'TimeWindow') -> 'TimeWindow':
        """
        Returns the minimal window covers both this window and the given window.
        """
        return TimeWindow(min(self.start, other.start), max(self.end, other.end))

    @staticmethod
    def get_window_start_with_offset(timestamp: int, offset: int, window_size: int):
        """
        Method to get the window start for a timestamp.

        :param timestamp: epoch millisecond to get the window start.
        :param offset: The offset which window start would be shifted by.
        :param window_size: The size of the generated windows.
        :return: window start
        """
        return timestamp - (timestamp - offset + window_size) % window_size

    @staticmethod
    def merge_windows(windows: Iterable['TimeWindow'],
                      callback: 'MergingWindowAssigner.MergeCallback[TimeWindow]') -> None:
        """
        Merge overlapping :class`TimeWindow`.
        """
        sorted_windows = list(windows)
        sorted_windows.sort()
        merged = []
        current_merge = None
        current_merge_set = set()

        for candidate in sorted_windows:
            if current_merge is None:
                current_merge = candidate
                current_merge_set.add(candidate)
            elif current_merge.intersects(candidate):
                current_merge = current_merge.cover(candidate)
                current_merge_set.add(candidate)
            else:
                merged.append((current_merge, current_merge_set))
                current_merge = candidate
                current_merge_set = set()
                current_merge_set.add(candidate)

        if current_merge is not None:
            merged.append((current_merge, current_merge_set))

        for merge_key, merge_set in merged:
            if len(merge_set) > 1:
                callback.merge(merge_set, merge_key)

    def __hash__(self):
        return self.start + mod_inverse((self.end << 1) + 1)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.end == other.end \
            and self.start == other.start

    def __lt__(self, other: 'TimeWindow'):
        if not isinstance(other, TimeWindow):
            raise Exception("Does not support comparison with non-TimeWindow %s" % other)

        return self.start == other.start and self.end < other.end or self.start < other.start

    def __le__(self, other: 'TimeWindow'):
        return self.__eq__(other) and self.__lt__(other)

    def __repr__(self):
        return "TimeWindow(start={}, end={})".format(self.start, self.end)

class EventTimeTrigger(Trigger[T, TimeWindow]):
    """
    A Trigger that fires once the watermark passes the end of the window to which a pane belongs.
    """

    def on_element(self,
                   element: T,
                   timestamp: int,
                   window: TimeWindow,
                   ctx: 'Trigger.TriggerContext') -> TriggerResult:
        if window.max_timestamp() <= ctx.get_current_watermark():
            return TriggerResult.FIRE
        else:
            ctx.register_event_time_timer(window.max_timestamp())
            # No action is taken on the window.
            return TriggerResult.CONTINUE

    def on_processing_time(self,
                           time: int,
                           window: TimeWindow,
                           ctx: 'Trigger.TriggerContext') -> TriggerResult:
        # No action is taken on the window.
        return TriggerResult.CONTINUE

    def on_event_time(self,
                      time: int,
                      window: TimeWindow,
                      ctx: 'Trigger.TriggerContext') -> TriggerResult:
        if time == window.max_timestamp():
            return TriggerResult.FIRE
        else:
            # No action is taken on the window.
            return TriggerResult.CONTINUE

    def can_merge(self) -> bool:
        return True

    def on_merge(self,
                 window: TimeWindow,
                 ctx: 'Trigger.OnMergeContext') -> None:
        window_max_timestamp = window.max_timestamp()
        if window_max_timestamp > ctx.get_current_watermark():
            ctx.register_event_time_timer(window_max_timestamp)

    def clear(self,
              window: TimeWindow,
              ctx: 'Trigger.TriggerContext') -> None:
        ctx.delete_event_time_timer(window.max_timestamp())

    @staticmethod
    def create() -> 'EventTimeTrigger':
        return EventTimeTrigger()

class SlidingEventTimeWindows(WindowAssigner[T, TimeWindow]):
    """
    A WindowAssigner that windows elements into sliding windows based on the timestamp of the
    elements. Windows can possibly overlap.

    For example, in order to window into windows of 1 minute, every 10 seconds:

    ::

        >>> data_stream.key_by(lambda x: x[0], key_type=Types.STRING()) \\
        ...     .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
    """

    def __init__(self, size: int, slide: int, offset: int):
        if abs(offset) >= slide or size <= 0:
            raise Exception("SlidingEventTimeWindows parameters must satisfy "
                            + "abs(offset) < slide and size > 0")

        self._size = size
        self._slide = slide
        self._offset = offset
        self._pane_size = math.gcd(size, slide)

    @staticmethod
    def of(size: Time, slide: Time, offset: Time = None) -> 'SlidingEventTimeWindows':
        """
        Creates a new :class:`SlidingEventTimeWindows` :class:`WindowAssigner` that assigns elements
        to time windows based on the element timestamp and offset.

        For example, if you want window a stream by hour,but window begins at the 15th minutes of
        each hour, you can use of(Time.hours(1),Time.minutes(15)),then you will get time
        windows start at 0:15:00,1:15:00,2:15:00,etc.

        Rather than that, if you are living in somewhere which is not using UTC±00:00 time, such as
        China which is using UTC+08:00, and you want a time window with size of one day, and window
        begins at every 00:00:00 of local time,you may use of(Time.days(1),Time.hours(-8)).
        The parameter of offset is Time.hours(-8) since UTC+08:00 is 8 hours earlier than UTC time.

        :param size The size of the generated windows.
        :param slide The slide interval of the generated windows.
        :param offset The offset which window start would be shifted by.
        :return The time policy.
        """
        if offset is None:
            return SlidingEventTimeWindows(size.to_milliseconds(), slide.to_milliseconds(), 0)
        else:
            return SlidingEventTimeWindows(size.to_milliseconds(), slide.to_milliseconds(),
                                           offset.to_milliseconds())

    def assign_windows(self,
                       element: T,
                       timestamp: int,
                       context: 'WindowAssigner.WindowAssignerContext') -> Collection[TimeWindow]:
        if timestamp > MIN_LONG_VALUE:
            last_start = TimeWindow.get_window_start_with_offset(timestamp,
                                                                 self._offset, self._slide)
            windows = [TimeWindow(start, start + self._size)
                       for start in range(last_start, timestamp - self._size, -self._slide)]
            return windows
        else:
            raise Exception("Record has Java Long.MIN_VALUE timestamp (= no timestamp marker). "
                            + "Is the time characteristic set to 'ProcessingTime', "
                              "or did you forget to call "
                            + "'data_stream.assign_timestamps_and_watermarks(...)'?")

    def get_default_trigger(self, env) -> Trigger[T, TimeWindow]:
        return EventTimeTrigger()

    def get_window_serializer(self) -> TypeSerializer[TimeWindow]:
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "SlidingEventTimeWindows(%s, %s, %s)" % (self._size, self._slide, self._offset)
