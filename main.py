import json
import os  # <-- ADDED for Kubernetes environment variables
import random
from datetime import datetime

from confluent_kafka import Producer
from fastapi import FastAPI
from fastapi.responses import FileResponse

# --- OPENTELEMETRY INITIALIZATION ---
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pydantic import BaseModel

# --- KUBERNETES ENV VARIABLES ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
JAEGER_ENDPOINT = os.getenv("JAEGER_ENDPOINT_URL", "http://jaeger:4317")

resource = Resource(attributes={"service.name": "enterprise-api-gateway"})
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint=JAEGER_ENDPOINT, insecure=True)  # <-- UPDATED
)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

ConfluentKafkaInstrumentor().instrument()
# -----------------------------------

TOPIC = "ai-telemetry-topic"
producer = Producer({"bootstrap.servers": KAFKA_BROKER})  # <-- UPDATED

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)


class ChatPayload(BaseModel):
    prompt: str


# Local fallback state for the cockpit telemetry view
LATEST_METRICS_CACHE = {
    "latest": {
        "timestamp": "--",
        "model": "SYS_STATUS: ACTIVE_AWAITING_TELEMETRY",
        "input_tokens": 0,
        "output_tokens": 0,
        "ttft": 0.0,
        "tps": 0.0,
    },
    "average_tps": 0.0,
}

# Add a local history list to track the UI average
tps_history = []


@app.post("/api/chat")
async def process_chat_message(payload: ChatPayload):
    word_tokens = len(payload.prompt.split())
    calculated_input_tokens = max(word_tokens, max(1, round(len(payload.prompt) / 4)))

    simulated_output_tokens = random.randint(32, 256)
    simulated_ttft = round(random.uniform(0.15, 0.65), 3)
    simulated_tps = round(random.uniform(24.0, 40.0), 2)

    telemetry_log = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "model": "compute-cluster-node-alpha",
        "input_tokens": calculated_input_tokens,
        "output_tokens": simulated_output_tokens,
        "ttft": simulated_ttft,
        "tps": simulated_tps,
    }

    # Forward exclusively to the Kafka Event Bus
    producer.produce(TOPIC, value=json.dumps(telemetry_log).encode("utf-8"))
    # producer.poll(0)
    producer.flush()

    # --- THE FIX: Calculate rolling average locally for the UI ---
    global tps_history
    tps_history.append(simulated_tps)
    if len(tps_history) > 10:
        tps_history.pop(0)

    rolling_avg = round(sum(tps_history) / len(tps_history), 2)

    # Update local API gateway cache for UI demonstration transparency
    LATEST_METRICS_CACHE["latest"] = telemetry_log
    LATEST_METRICS_CACHE["average_tps"] = rolling_avg
    # -------------------------------------------------------------

    return {
        "status": "SUCCESS",
        "message": f"[STATUS: TRANSACTION_SUCCESS] Telemetry payload packet successfully submitted to Kafka topic partition broker. Ingested token count: {calculated_input_tokens}.",
    }


@app.get("/")
async def serve_ui():
    return FileResponse("index.html")


# 2. This serves the metrics back to the UI's 1000ms polling loop
@app.get("/api/latest-data")
async def get_latest_data():
    return LATEST_METRICS_CACHE
