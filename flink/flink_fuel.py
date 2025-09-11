from pyflink.common import Row,Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy,TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import SlidingProcessingTimeWindows 
from pyflink.datastream.functions import ProcessWindowFunction,SourceFunction
from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import 
from pyflink.datastream.connectors.kafka import KafkaSource,KafkaOffsetsInitializer
import socket
import json
BURN_WINDOW = 0.5
FIELD={
    "event_ts":2
}
class FirstElementTimestampAssigner(TimestampAssigner):
    def __init__(self):
        pass
        # super.__init__(TimestampAssigner())


    def extract_timestamp(self, row, record_timestamp):
        print("RSK",row,record_timestamp)
        return int(row[FIELD["event_ts"]]*1000)

def get_source():
    source = KafkaSource.builder()\
    .set_bootstrap_servers("localhost:9092")\
    .set_topics("car_status_telemetry")\
    .set_key_deserializer(SimpleStringSchema())\
    .set_value_deserializer(SimpleStringSchema())\
    .set_starting_offsets_initializer(KafkaOffsetsInitializer.earliest())\
    .build()
    return source


def fuel_monitor():
    env = StreamExecutionEnvironment.get_execution_environment()
    current_dir_list = __file__.split("/")[:-1]
    current_dir = "/".join(current_dir_list)
    print(f"Current directory: {current_dir}")  
    env.add_jars(
        f"file://{current_dir}/flink-sql-connector-kafka-4.0.0-2.0.jar"
    )
    earliest = False
    offset = (
        KafkaOffsetsInitializer.earliest()
        if earliest
        else KafkaOffsetsInitializer.latest()
    )
    kafka_source = (
        KafkaSource.builder()
        .set_topics("car_status_telemetry")
        .set_bootstrap_servers("localhost:9092")
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    datastream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka"
    )
    # datastream.print()

    mapped_stream = datastream.map(
        lambda x: Row(
            json.loads(x)["player_car_index"],
            json.loads(x)["fuel_in_tank"],
            json.loads(x)["event_ts"]
        ),
        output_type=Types.ROW_NAMED(
        ["player_car_index", "fuel_in_tank", "event_ts"],
        [Types.INT(), Types.DOUBLE(), Types.DOUBLE()]
    )
    )
    
    # datastream.print()
    # mapped_stream.print()
    timestamp_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(FirstElementTimestampAssigner())
    datastream_with_timestamps = mapped_stream.assign_timestamps_and_watermarks(timestamp_strategy)
    datastream_with_timestamps.print()

    env.execute("Fuel Monitor Job")
    

#     sliding_stream.print()

# class FuelConsumptionCalculator(ProcessWindowFunction):
#     def process(self,key,context,elements):
#         sorted_elements = sorted(list(elements),key=lambda x:x["session_time"])
#         if len(sorted_elements)>1:
#             first = sorted_elements[0]
#             last= sorted_elements[-1]
#             time_diff = last["session_time"] - first["session_time"]
#             fuel_diff = time_diff = first["fuel_in_tank"] - last["fuel_in_tank"]
#             if time_diff<=0:
#                 yield Row(car_index=key,fuel_burn_rate=0.0,category="Unknown")
#             else:
#                 yield Row(car_index=key,fuel_burn_rate=fuel_diff/time_diff,category="Unknown")
#         else:
#             yield Row(car_index=key,fuel_burn_rate=0.0,category="Unknown")

if __name__ == "__main__":
    fuel_monitor()
