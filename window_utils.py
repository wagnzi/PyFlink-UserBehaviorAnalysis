from abc import ABC, abstractmethod
from typing import Union, Iterable, Generic

from pyflink.common import TypeInformation, Types
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import DataStream, WindowFunction, ProcessWindowFunction, AggregateFunction, \
    RuntimeContext, WindowedStream
from pyflink.datastream.data_stream import _get_one_input_stream_operator
from pyflink.datastream.functions import KEY, OUT, IN, W, \
    InternalProcessWindowContext, Function, KeyedStateStore
from pyflink.datastream.state import AggregatingStateDescriptor, StateDescriptor
from pyflink.datastream.window import WindowOperationDescriptor

class InternalWindowFunction(Function, Generic[IN, OUT, KEY, W]):

    class InternalWindowContext(ABC):

        @abstractmethod
        def current_processing_time(self) -> int:
            pass

        @abstractmethod
        def current_watermark(self) -> int:
            pass

        @abstractmethod
        def window_state(self) -> KeyedStateStore:
            pass

        @abstractmethod
        def global_state(self) -> KeyedStateStore:
            pass

    @abstractmethod
    def process(self,
                key: KEY,
                window: W,
                context: InternalWindowContext,
                input_data: IN) -> Iterable[OUT]:
        pass

    @abstractmethod
    def clear(self, window: W, context: InternalWindowContext):
        pass

class InternalSingleValueWindowFunction(InternalWindowFunction[IN, OUT, KEY, W]):

    def __init__(self, wrapped_function: WindowFunction):
        self._wrapped_function = wrapped_function

    def open(self, runtime_context: RuntimeContext):
        self._wrapped_function.open(runtime_context)

    def close(self):
        self._wrapped_function.close()

    def process(self,
                key: KEY,
                window: W,
                context: InternalWindowFunction.InternalWindowContext,
                input_data: IN) -> Iterable[OUT]:
        return self._wrapped_function.apply(key, window, [input_data])

    def clear(self, window: W, context: InternalWindowFunction.InternalWindowContext):
        pass


class PassThroughWindowFunction(WindowFunction[IN, IN, KEY, W]):

    def apply(self, key: KEY, window: W, inputs: Iterable[IN]) -> Iterable[IN]:
        yield from inputs


class InternalSingleValueProcessWindowFunction(InternalWindowFunction[IN, OUT, KEY, W]):

    def __init__(self, wrapped_function: ProcessWindowFunction):
        self._wrapped_function = wrapped_function
        self._internal_context = \
            InternalProcessWindowContext()  # type: InternalProcessWindowContext

    def open(self, runtime_context: RuntimeContext):
        self._wrapped_function.open(runtime_context)

    def close(self):
        self._wrapped_function.close()

    def process(self,
                key: KEY,
                window: W,
                context: InternalWindowFunction.InternalWindowContext,
                input_data: IN) -> Iterable[OUT]:
        self._internal_context._window = window
        self._internal_context._underlying = context
        return self._wrapped_function.process(key, self._internal_context, [input_data])

    def clear(self, window: W, context: InternalWindowFunction.InternalWindowContext):
        self._internal_context._window = window
        self._internal_context._underlying = context
        self._wrapped_function.clear(self._internal_context)


def aggregate(windowed_stream: WindowedStream,
              aggregate_function: AggregateFunction,
              window_function: Union[WindowFunction, ProcessWindowFunction] = None,
              accumulator_type: TypeInformation = None,
              output_type: TypeInformation = None) -> DataStream:
    """
    Applies the given window function to each window. The window function is called for each
    evaluation of the window for each key individually. The output of the window function is
    interpreted as a regular non-windowed stream.
    Arriving data is incrementally aggregated using the given aggregate function. This means
    that the window function typically has only a single value to process when called.
    Example:
    ::
        >>> class AverageAggregate(AggregateFunction):
        ...     def create_accumulator(self) -> Tuple[int, int]:
        ...         return 0, 0
        ...
        ...     def add(self, value: Tuple[str, int], accumulator: Tuple[int, int]) \\
        ...             -> Tuple[int, int]:
        ...         return accumulator[0] + value[1], accumulator[1] + 1
        ...
        ...     def get_result(self, accumulator: Tuple[int, int]) -> float:
        ...         return accumulator[0] / accumulator[1]
        ...
        ...     def merge(self, a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
        ...         return a[0] + b[0], a[1] + b[1]
        >>> ds.key_by(lambda x: x[1]) \\
        ...     .window(TumblingEventTimeWindows.of(Time.seconds(5))) \\
        ...     .aggregate(AverageAggregate(),
        ...                accumulator_type=Types.TUPLE([Types.LONG(), Types.LONG()]),
        ...                output_type=Types.DOUBLE())
    :param aggregate_function: The aggregation function that is used for incremental
                               aggregation.
    :param window_function: The window function.
    :param accumulator_type: Type information for the internal accumulator type of the
                             aggregation function.
    :param output_type: Type information for the result type of the window function.
    :return: The data stream that is the result of applying the window function to the window.
    .. versionadded:: 1.16.0
    """
    if window_function is None:
        internal_window_function = InternalSingleValueWindowFunction(
            PassThroughWindowFunction())  # type: InternalWindowFunction
    elif isinstance(window_function, WindowFunction):
        internal_window_function = InternalSingleValueWindowFunction(window_function)
    elif isinstance(window_function, ProcessWindowFunction):
        internal_window_function = InternalSingleValueProcessWindowFunction(window_function)
    else:
        raise TypeError("window_function should be a WindowFunction or ProcessWindowFunction")

    if accumulator_type is None:
        accumulator_type = Types.PICKLED_BYTE_ARRAY()
    elif isinstance(accumulator_type, list):
        accumulator_type = RowTypeInfo(accumulator_type)

    aggregating_state_descriptor = AggregatingStateDescriptor('window-contents',
                                                              aggregate_function,
                                                              accumulator_type)

    def _get_result_data_stream(windowed_stream: WindowedStream,
                                internal_window_function: InternalWindowFunction,
                                window_state_descriptor: StateDescriptor,
                                output_type: TypeInformation):
        if windowed_stream._window_trigger is None:
            windowed_stream._window_trigger = windowed_stream._window_assigner.get_default_trigger(
                windowed_stream.get_execution_environment())
        window_serializer = windowed_stream._window_assigner.get_window_serializer()
        window_operation_descriptor = WindowOperationDescriptor(
            windowed_stream._window_assigner,
            windowed_stream._window_trigger,
            windowed_stream._allowed_lateness,
            window_state_descriptor,
            window_serializer,
            internal_window_function)

        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                windowed_stream._keyed_stream,
                window_operation_descriptor,
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.WINDOW,  # type: ignore
                output_type)

        return DataStream(windowed_stream._keyed_stream._j_data_stream.transform(
            "WINDOW",
            j_output_type_info,
            j_python_data_stream_function_operator))

    return _get_result_data_stream(windowed_stream,
                                   internal_window_function,
                                   aggregating_state_descriptor,
                                   output_type)
