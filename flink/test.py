import json

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


# def parse_and_filter(value: str) -> str | None:
#     return None


def main() -> None:
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Get current directory
    current_dir_list = __file__.split("/")[:-1]
    current_dir = "/".join(current_dir_list)
    print(f"Current directory: {current_dir}")  

    # Adding the jar to the flink streaming environment
    env.add_jars(
        f"file://{current_dir}/flink-sql-connector-kafka-4.0.0-2.0.jar"
    )

    properties = {
        "bootstrap.servers": "kafka:19092",
        "group.id": "iot-sensors",
    }

    earliest = False
    offset = (
        KafkaOffsetsInitializer.earliest()
        if earliest
        else KafkaOffsetsInitializer.latest()
    )

    # Create a Kafka Source
    # NOTE: FlinkKafkaConsumer class is deprecated
    kafka_source = (
        KafkaSource.builder()
        .set_topics("car_status_telemetry")
        .set_bootstrap_servers("localhost:9092")
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka sensors topic"
    )
    print("DataStream:", data_stream)
main()