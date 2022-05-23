from typing import Any

from pyflink.common import Duration, Row
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.table.expressions import col
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common.typeinfo import Types
from pyflink.table.window import Slide
from pyflink.table import expressions as expr
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy


def split_data(row):
    line = row.split(',')
    user_id, item_id, category_id, behavior, timestamp = int(line[0]), int(line[1]), int(line[2]), line[3], int(line[4])
    yield Row(user_id, item_id, category_id, behavior, timestamp)


def hot_items_stat_sql():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    input_stream = env.read_text_file('UserBehavior.csv')
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(RowTimestampAssigner())
    type_info = Types.ROW_NAMED(["user_id", "item_id", "category_id", "behavior", "timestamp"], [Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.LONG()])
    data_stream = input_stream.flat_map(split_data, output_type=type_info)
    data_stream = data_stream.assign_timestamps_and_watermarks(watermark_strategy)

    table_env = StreamTableEnvironment.create(env)

    # 获取系统时间作为时间属性
    # data_table = table_env.from_data_stream(data_stream, Schema.new_builder().column("f1", DataTypes.INT())
    #                                         .column("f3", DataTypes.STRING())
    #                                         .column_by_expression("ts", "PROCTIME()").build()).alias('item_id','behavior', 'ts')

    # 获取timestamp的事件时间,api形式
    # data_table = table_env.from_data_stream(data_stream, col("item_id"), col("behavior"), col("timestamp").rowtime.alias("ts"))
    #
    # agg_table = data_table.filter("behavior == 'pv'")\
    #     .window(Slide.over(expr.lit(1).hours).every(expr.lit(5).minutes).on(col("ts")).alias("sw"))\
    #     .group_by(col("item_id"), col("sw"))\
    #     .select(col("item_id"), col("sw").end.alias("windowEnd"), col("item_id").count.alias("cnt"))
    #
    # table_env.create_temporary_view("aggTable", agg_table, col("item_id"), col("windowEnd"), col("cnt"))
    # result_table = table_env.sql_query("""
    #     select * from (
    #         select *, row_number() over (partition by windowEnd order by cnt desc) as row_num
    #         from aggTable
    #     ) where row_num <= 5
    # """.strip())



    # 纯SQL实现
    table_env.create_temporary_view("data_table", data_stream, col("item_id"), col("behavior"), col("timestamp").rowtime.alias("ts"))

    result_table = table_env.sql_query("""
        select *
        from (
            select *, row_number() over (partition by windowEnd order by cnt desc) as row_num
            from (
                select item_id, hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd, count(item_id) as cnt
                from data_table
                where behavior = 'pv'
                group by
                    item_id,
                    hop(ts, interval '5' minute, interval '1' hour)
            )
        ) where row_num <= 5
    """)
    result_stream = table_env.to_retract_stream(result_table, Types.TUPLE([Types.INT(), Types.INSTANT(), Types.BIG_DEC(), Types.BIG_DEC()]))
    result_stream.print()
    env.execute("hot items sql job")


class RowTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return int(value[4]) * 1000

if __name__ == '__main__':
    hot_items_stat_sql()
