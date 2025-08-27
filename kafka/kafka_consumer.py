from quixstreams import Application
import json
app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",
    consumer_group="dash"
)

topic = "car_telemetry"

with app.get_consumer() as consumer:
    consumer.subscribe([topic])
    print(f"Polling Kafka topic '{topic}'...")
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None:
            key = msg.key().decode('utf-8')
            value = json.loads(msg.value().decode('utf-8'))
            print(key,value)


    # t = threading.Thread(target=main, args=(dashboard,), daemon=True)
    # t.start()
    # dashboard.run()