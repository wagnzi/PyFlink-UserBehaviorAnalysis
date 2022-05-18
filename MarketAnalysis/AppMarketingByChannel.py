from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic




def app_market_by_channel():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)




if __name__ == '__main__':
    app_market_by_channel()
