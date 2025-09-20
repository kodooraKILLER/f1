from pyflink.common import Row
from pyflink.common import Time
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy,TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import SlidingEventTimeWindows 
from pyflink.datastream.functions import AggregateFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource,KafkaOffsetsInitializer,KafkaSink,KafkaRecordSerializationSchema
import socket
import json
BURN_WINDOW = 0.5
FIELD={
    "player_car_index":0,
    "fuel_in_tank":1,
    "event_ts":2,
}

def json_to_row(json_str):
    ele = json.loads(json_str)
    return Row(
            ele["player_car_index"],
            ele["fuel_in_tank"],
            ele["event_ts"]
        )

class FuelAggregator(AggregateFunction): 

    def create_accumulator(self):
        latest_ts = None
        start_ts = None
        latest_fuel = None
        start_fuel = None
        count = 0
        return start_fuel,latest_fuel,start_ts,latest_ts,count

    def add(self, value, accumulator):
        latest_ts = accumulator[3]
        start_ts = accumulator[2]
        latest_fuel = accumulator[1]
        start_fuel = accumulator[0]
        count = accumulator[4]
        
        if start_fuel is None:
            print("RSK START",value,accumulator)    
            return value[FIELD["fuel_in_tank"]],value[FIELD["fuel_in_tank"]],value[FIELD["event_ts"]],value[FIELD["event_ts"]],1
        if latest_ts <= value[FIELD["event_ts"]]:
            print("RSK LATEST",value,accumulator)    
            return start_fuel,value[FIELD["fuel_in_tank"]],start_ts,value[FIELD["event_ts"]],count+1
        if start_ts >= value[FIELD["event_ts"]]:
            print("RSK EARLIEST",value,accumulator)
            return value[FIELD["fuel_in_tank"]],latest_fuel,value[FIELD["event_ts"]],latest_ts,count+1
        print("RSK BLEH",value,accumulator)
        return start_fuel,latest_fuel,start_ts,latest_ts,count+1
        

    def get_result(self, accumulator):
        latest_ts = accumulator[3]
        start_ts = accumulator[2]
        latest_fuel = accumulator[1]
        start_fuel = accumulator[0]
        counter = accumulator[4]
        if latest_ts is None:
            print("RSK <> 69 EMPTY")
            return None
        if start_ts==latest_ts:
            print("RSK << 69 SINGLE",counter)
            return None
        print("RSK << 69 CALC")
        fuel_burn_rate = (start_fuel-latest_fuel)*1000/((latest_ts-start_ts))
        print("RSK <}",fuel_burn_rate,latest_ts,counter)
        return Row(
            fuel_burn=fuel_burn_rate,
            event_ts=latest_ts,
            cnt=counter
            )

    def merge(self, a,b):
        if a[0] is None:
            return b
        if b[0] is None:
            return a
        
        if a[3]>=b[3]:
            g = a[1]
            y = a[3]
        else:
            g = b[1]
            y = b[3]
        
        if a[2]<=b[2]:
            f = a[0]
            x = a[2]
        else:
            f = b[0]
            x = b[2]

        cnt = a[4]+b[4]
        return f,g,x,y,cnt




def parse_fuel_event(event):
    event_json = json.loads(event)
    return Row(
            event_json["player_car_index"],
            event_json["fuel_in_tank"],
            event_json["event_ts"]
        ),
class FirstElementTimestampAssigner(TimestampAssigner):
    def __init__(self):
        pass


    def extract_timestamp(self, row, record_timestamp):
        return int(row[FIELD["event_ts"]]*1000)



def fuel_monitor():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_buffer_timeout(0) 
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
        lambda x: json_to_row(x),
        output_type=Types.ROW_NAMED(
        ["player_car_index", "fuel_in_tank", "event_ts"],
        [Types.INT(), Types.DOUBLE(), Types.DOUBLE()]
    )
    )
    
    timestamp_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(FirstElementTimestampAssigner())
    datastream_with_timestamps = mapped_stream.assign_timestamps_and_watermarks(timestamp_strategy)
    # datastream_with_timestamps.print()
    

    sliding_stream = datastream_with_timestamps.key_by(lambda x: x[FIELD["player_car_index"]])\
                        .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(200)))
    
    stream_of_aggregates = sliding_stream.aggregate(
        FuelAggregator(),
        accumulator_type=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE(),Types.DOUBLE(), Types.DOUBLE()]),
        output_type=Types.ROW_NAMED(["fuel_burn", "event_ts","cnt"], [Types.DOUBLE(), Types.DOUBLE(),Types.DOUBLE()]))
    
    

    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("fuel-burner")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
        .build()
    )
    result_stream = stream_of_aggregates.map(lambda x: f'{{"fuel_burn": {x["fuel_burn"]}, "event_ts": {x["event_ts"]}}}', output_type=Types.STRING())
    result_stream.print()
        
    result_stream.sink_to(kafka_sink)

    job_client = env.execute_async("Fuel Monitor Job")
    print(f"Job ID: {job_client.get_job_id()}")
    return job_client
    


if __name__ == "__main__":
    try:
        job_client = fuel_monitor()
        while True:
            status = job_client.get_job_status()
            print(f"Job Status: {status}")
            # If the job is in a final state, exit the loop
            if status in ["FINISHED", "CANCELED", "FAILED"]:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping job...")
        try:
            job_client.cancel()
            print("Job canceled successfully.")
        except Exception as e:
            print(f"Failed to cancel job: {e}")
