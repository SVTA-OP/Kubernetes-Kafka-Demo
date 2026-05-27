import json
import os  # <-- ADDED for Kubernetes environment variables
import sys

# Removed TopicPartition and OFFSET_END, they are no longer needed for subscribe()
from confluent_kafka import Consumer, KafkaException

# --- OPENTELEMETRY INITIALIZATION ---
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# --- KUBERNETES ENV VARIABLES ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
JAEGER_ENDPOINT = os.getenv("JAEGER_ENDPOINT_URL", "http://jaeger:4317")

resource = Resource(attributes={"service.name": "enterprise-consumer-worker"})
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint=JAEGER_ENDPOINT, insecure=True)  # <-- UPDATED
)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

ConfluentKafkaInstrumentor().instrument()
# -----------------------------------

TOPIC = "ai-telemetry-topic"
tps_history = []


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,  # <-- UPDATED
            "group.id": "enterprise-isolated-worker-group",
            "auto.offset.reset": "latest",
        }
    )

    print(
        "2026-05-20 14:45:00,000 [INFO] (StandaloneWorker) Initializing independent consumer pipeline..."
    )

    # --- THE KUBERNETES RESILIENCY FIX ---
    # Subscribe allows the consumer to patiently wait for the topic to exist
    consumer.subscribe([TOPIC])
    # -------------------------------------

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(
                    f"[ERROR] Kafka consumption anomaly: {msg.error()}", file=sys.stderr
                )
                continue

            log = json.loads(msg.value().decode("utf-8"))

            # Perform stream processing metrics calculations
            tps_history.append(log["tps"])
            if len(tps_history) > 10:
                tps_history.pop(0)

            rolling_avg = round(sum(tps_history) / len(tps_history), 2)

            # Formalized log statement confirming ingestion downstream
            print(
                f"2026-05-20 14:45:01,000 [INFO] (StandaloneWorker) STREAM_PROCESSING_SUCCESS | Origin: {log['model']} | Window Avg TPS: {rolling_avg}"
            )

            # NOTE: In a multi-container cluster setup, you would add a line here to save
            # these calculations to Redis, which main.py would read from.

    except KeyboardInterrupt:
        print("\n[INFO] Terminating consumer service process gracefully.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
