import streamlit as st
import json
from kafka import KafkaConsumer
import time

def get_kafka_consumer():
    return KafkaConsumer(
        'fuel-burner',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: x.decode('utf-8')
    )

st.set_page_config(
    page_title="Live F1 Fuel Burn Rate",
    layout="centered",
    initial_sidebar_state="collapsed",
)

st.title("Live F1 Fuel Burn Rate Dashboard")
st.markdown("This dashboard displays the live fuel burn rate calculated by the Flink job.")

# Create a placeholder to update the metric
fuel_rate_placeholder = st.empty()

# Create a container for live data
live_data_container = st.container()

with live_data_container:
    st.header("Current Fuel Burn Rate")
    # Use a metric to display the value prominently
    metric_box = st.metric(label="Fuel Burn Rate", value="Waiting for data...", delta=None)
    ts_box = st.metric(label="Timestamp", value="Waiting for data...", delta=None)

# Get the consumer instance
consumer = get_kafka_consumer()

try:
    # Continuously poll for messages and update the display
    for message in consumer:
        try:
            json_data = json.loads(message.value)
            fuel_rate = json_data.get("fuel_burn")
            event_ts = json_data.get("event_ts")
            timedelta = time.time() - event_ts
            
            # Update the metric with the new value
            metric_box.metric(label="Fuel Burn Rate (L/s)", value=f"{fuel_rate:.2f}", delta=None)
            ts_box.metric(label="timedelta", value=f"{timedelta:.2f}", delta=None)
            
        except (ValueError, TypeError) as e:
            st.error(f"Error parsing message: {e}")
            continue
        
        # Use a short sleep to prevent the app from consuming too many resources
        # and to give the UI a chance to refresh.
    time.sleep(0.1)

except Exception as e:
    st.error(f"Could not connect to Kafka: {e}")
    st.stop()
