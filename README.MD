# Enterprise Architecture PoC — Inference Telemetry Engine

### Kubernetes Deployment Guide

---

## Overview

A full-stack observability platform deployed on Kubernetes that simulates an AI inference telemetry pipeline. A FastAPI gateway accepts user prompts, produces telemetry events to a Kafka broker, a background consumer worker processes the stream and calculates rolling TPS averages, and all distributed traces are exported to Jaeger via OpenTelemetry.

---

## Architecture

```text
Browser (index.html)
    │
    │  HTTP POST /api/chat
    ▼
FastAPI Gateway (main.py)          ──► Jaeger (OTLP traces)
    │
    │  produce(ai-telemetry-topic)
    ▼
Kafka Broker (KRaft mode)
    │
    │  subscribe(ai-telemetry-topic)
    ▼
Consumer Worker (consumer.py)      ──► Jaeger (OTLP traces)
    │
    ▼
stdout: STREAM_PROCESSING_SUCCESS logs

```

---

## Prerequisites

* **Docker**: For building the local container images.
* **Kubernetes**: A local cluster like Minikube, with `kubectl` configured.
* **Helm**: (Optional) Package manager for Kubernetes.

---

## Project File Structure

```text
Kubernetes/
├── Dockerfile              # Builds the Python app image
├── requirements.txt        # Python dependencies
├── main.py                 # FastAPI producer + telemetry gateway
├── consumer.py             # Kafka consumer worker
├── index.html              # Frontend dashboard UI
├── kafka.yaml              # Kafka StatefulSet + headless Service
├── jaeger.yaml             # Jaeger Deployment + NodePort Service
└── python-apps.yaml        # FastAPI + Consumer Deployments with initContainers

```

---

## Step 1 — Build the Docker Image

From inside the `Kubernetes/` directory, build the image natively so Kubernetes can access it:

```bash
# Point your terminal to Minikube's Docker daemon first
eval $(minikube docker-env)

# Build and tag the image
docker build -t enterprise-telemetry-app:v2 .

```

Verify the files (`main.py`, `consumer.py`, `index.html`) are inside the image:

```bash
docker run --rm enterprise-telemetry-app:v2 ls /app

```

> **Note:** We use `imagePullPolicy: Never` in the `python-apps.yaml` so Kubernetes uses this local image instead of trying to pull from the internet.

---

## Step 2 — Deploy Infrastructure

Deploy Jaeger (OpenTelemetry backend) and Kafka (Message Broker):

```bash
kubectl apply -f jaeger.yaml
kubectl apply -f kafka.yaml

```

Wait for Kafka to be fully ready before proceeding:

```bash
kubectl get pods -w
# Wait until kafka-0 shows 1/1 Running

```

---

## Step 3 — Deploy FastAPI Gateway and Consumer Worker

```bash
kubectl apply -f python-apps.yaml
kubectl get pods -w

```

Both pods use an `initContainer` that runs a network check (`until nc -z kafka 9092...`). This blocks the Python applications from starting until Kafka is fully accepting connections.

---

## Accessing the Application (Minikube Networking)

When using Minikube, `NodePort` services are exposed on the Minikube virtual machine's IP address, not `localhost`.

**To find your Minikube IP address, run:**

```bash
minikube ip

```

*(Example output: `192.168.49.2`)*

Alternatively, you can have Minikube automatically generate the exact URL for a service:

```bash
minikube service fastapi-service --url
minikube service jaeger --url

```

### Network Routing Table

| Component | Internal K8s DNS | Internal Port | External Access (NodePort) |
| --- | --- | --- | --- |
| **FastAPI Gateway** | `kafka:9092` | 8000 | `http://<MINIKUBE_IP>:30000` |
| **Jaeger UI** | `jaeger` | 16686 | `http://<MINIKUBE_IP>:30686` |
| **Kafka Broker** | `kafka.default.svc.cluster.local` | 9092 | Internal only |
| **Kafka Controller** | `kafka-0.kafka.default.svc.cluster.local` | 9093 | Internal only |
| **Jaeger OTLP** | `jaeger` | 4317 | Internal (gRPC traces) |

**To test the pipeline:**

1. Open `http://192.168.49.2:30000` (Replace with your exact Minikube IP).
2. Type a prompt and hit **EXECUTE**.
3. Run `kubectl logs -f -l app=consumer` to watch the data process in real-time.

---

## Troubleshooting Guide

If the cluster fails to start or messages do not propagate, consult these specific failure states observed during development:

### 1. Pods Crashing with `[Errno 2] No such file or directory`

* **Symptoms:** The FastAPI or Consumer pods enter `CrashLoopBackOff`. Logs reveal Python cannot find `main.py` or `consumer.py`.
* **Root Cause:** When migrating from Docker Compose, the `volumes: - .:/app` mapping was lost. Kubernetes cannot read local host directories dynamically.
* **Fix:** Ensure your `Dockerfile` includes `COPY . .` after installing dependencies, then rebuild the image using `docker build -t enterprise-telemetry-app:v2 .` and update the deployment YAML to use the `v2` tag.

### 2. Kafka Pod Stuck in `Init:ImagePullBackOff`

* **Symptoms:** Kafka refuses to start. `kubectl describe pod kafka-0` shows manifest unknown or pull rate limits.
* **Root Cause:** Using the Bitnami Kafka Helm chart without a commercial license limits pulls for their `latest` tags.
* **Fix:** Abandon the Bitnami chart and use a manual `StatefulSet` deployment pulling the official, free `apache/kafka:latest` image.

### 3. App Updated, but Kubernetes Ignoring Changes

* **Symptoms:** `kubectl apply -f python-apps.yaml` says `unchanged` despite code modifications.
* **Root Cause:** Kubernetes determines updates based on changes to the YAML manifest. If the image tag (`:v1`) remains the same, K8s assumes the container hasn't changed.
* **Fix:** Either bump the tag (e.g., build `:v3` and update the YAML) or forcefully restart the deployment:
```bash
kubectl rollout restart deployment fastapi-gateway kafka-consumer-worker

```



### 4. UI Shows "Success", but Consumer is Silent

* **Symptoms:** The web UI claims telemetry was submitted. However, `kubectl logs -l app=consumer` shows no output, and `kafka-console-consumer.sh` shows no data inside the broker.
* **Root Cause 1 (Kafka Missing Metadata):** In a single-node setup, Kafka defaults to a replication factor of 3 for its internal `__consumer_offsets` topic. If this topic fails to create, consumers cannot connect properly.
* **Fix:** Ensure the Kafka StatefulSet includes `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1"`.


* **Root Cause 2 (False Positive API):** The FastAPI `producer.produce()` command is operating in fire-and-forget mode. If the producer is using a stale connection, it drops the message but FastAPI still returns `200 OK`.
* **Fix:** Ensure the `initContainer` is actively blocking FastAPI from starting until `nc -z kafka 9092` passes, guaranteeing a fresh connection. Restart the gateway (`kubectl rollout restart deployment fastapi-gateway`).



---

## Teardown

To cleanly remove all components from your cluster:

```bash
kubectl delete -f python-apps.yaml
kubectl delete -f jaeger.yaml
kubectl delete statefulset kafka
kubectl delete svc kafka

```

Here is a categorized list of the specific commands used to troubleshoot, diagnose, and resolve the issues encountered during the Kubernetes migration.

### 1. Cluster & Pod Diagnostics

These commands were used to check the overall health of the Kubernetes cluster, inspect failing containers, and monitor state changes.

* **Watch pod status in real-time:**
```bash
kubectl get pods -w

```


*Why we used it:* To monitor the `ImagePullBackOff`, `CrashLoopBackOff`, and `Init:0/1` states as the pods attempted to start and connect to Kafka.
* **Inspect pod failure reasons:**
```bash
kubectl describe pod <pod-name>

```


*Why we used it:* To uncover the exact reason the Bitnami Kafka pods were failing to pull their images (rate limiting/missing manifest) and to check container exit codes.
* **Verify Service DNS & IP Allocation:**
```bash
kubectl get svc

```


*Why we used it:* To confirm that the internal Kubernetes service for Kafka had successfully received a `ClusterIP` and to ensure the NodePorts were correctly assigned.

### 2. Application Log Inspection

These commands isolated the Python applications to see what errors they were throwing internally or swallowing.

* **Tail consumer logs dynamically:**
```bash
kubectl logs -f -l app=consumer

```


*Why we used it:* To watch for the `STREAM_PROCESSING_SUCCESS` messages and to diagnose the initial `Connection refused` and `Name or service not known` DNS errors.
* **Tail FastAPI logs while filtering out spam:**
```bash
kubectl logs -l app=fastapi | grep -v "latest-data"

```


*Why we used it:* To filter out the noisy `GET /api/latest-data` UI polling requests and isolate the `_UNKNOWN_TOPIC` errors that the producer was throwing silently.

### 3. Kafka Internal Interrogation

These were the most critical commands. Because Kafka is a complex stateful system, we had to execute native Kafka shell scripts directly inside the broker pod to bypass the Python layer completely.

* **Check if the topic was successfully created:**
```bash
kubectl exec -it kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

```


*Why we used it:* To prove whether the FastAPI producer had successfully reached the broker to trigger auto-topic creation.
* **Inspect topic health and replication:**
```bash
kubectl exec -it kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ai-telemetry-topic

```


*Why we used it:* To check if the topic was healthy, which led to discovering that the `__consumer_offsets` topic was failing to create due to the replication factor mismatch.
* **Listen directly to the data stream (Bypassing Python):**
```bash
kubectl exec -it kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ai-telemetry-topic \
  --from-beginning

```


*Why we used it:* To definitively prove whether FastAPI was dropping the messages or the Python Consumer was simply failing to read them.
* **Measure physical data size on disk:**
```bash
kubectl exec -it kafka-0 -- /opt/kafka/bin/kafka-log-dirs.sh \
  --bootstrap-server localhost:9092 \
  --topic-list ai-telemetry-topic \
  --describe 2>/dev/null | grep -o '"size":[0-9]*'

```


*Why we used it:* To confirm that the actual JSON bytes were physically stored in Kafka's logs on the disk (which returned `"size":18788`), proving the data was there.

### 4. Workload Management & Forcing State

When Kubernetes caches old configurations or deadlocks a rollout, these commands force the cluster to adopt the new state.

* **Force applications to restart and drop stale connections:**
```bash
kubectl rollout restart deployment fastapi-gateway kafka-consumer-worker

```


*Why we used it:* Essential for clearing out the Python applications' cached DNS records and forcing them to re-run the `initContainer` checks after Kafka came back online.
* **Verify Image Contents:**
```bash
docker run --rm enterprise-telemetry-app:v2 ls /app

```


*Why we used it:* To prove that the `COPY . .` command in the Dockerfile had successfully bundled `main.py` and `consumer.py` into the container, fixing the `[Errno 2] No such file or directory` crash.
