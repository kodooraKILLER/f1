from pyflink.common import Row,Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import SlidingProcessingTimeWindows 
from pyflink.datastream.functions import ProcessWindowFunction,SourceFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource,KafkaOffsetsInitializer
import socket
import json
BURN_WINDOW = 0.5
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
    datastream.print()
    # row_type_info = Types.ROW_NAMED([Types.INT(), Types.FLOAT(), Types.FLOAT()], 
    #                          ["player_car_index", "fuel_in_tank", "time"])
    # mapped_stream = datastream.map(
    #     lambda x: Row(**json.loads(x)),
    #     output_type=row_type_info
    # )
    # keyed_stream = mapped_stream.key_by(lambda x: 1)
    # max_stream = keyed_stream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).max("player_car_index")
    # max_stream.print()
    env.execute("Fuel Monitor Job")
    
    # time_marked_stream = mapped_stream.assign_timestamps_and_watermarks(
    #     WatermarkStrategy.for_monotonous_timestamps()
    #     .with_timestamp_assigner(lambda row,record_timestamp: row["session_time"])
    # )

#     sliding_stream = time_marked_stream.key_by(lambda row: row["car_index"])\
#     .window(SildingEventTimeWindows.of(Time.seconds(BURN_WINDOW),Time.seconds(BURN_WINDOW)))\
#     .process(
#         FuelConsumptionCalculator()
#         ,output_type = Types.ROW_NAMED(
#                         ["car_index","fuel_burn_rate","category"],
#                         [Types.INT(),Types.FLOAT(),Types.STRING()]
#                         )
#     )

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
