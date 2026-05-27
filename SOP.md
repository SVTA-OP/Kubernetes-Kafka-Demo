# Architecture Migration Guide: Inference Telemetry Engine

**Document Purpose:** This guide outlines the standard operating procedures and architectural configurations required to migrate the Inference Telemetry Engine from a local Docker Compose environment to a resilient Kubernetes cluster.

---

## 1. Application Decoupling & Code Readiness

To ensure compatibility with dynamic Kubernetes orchestration, application code must be decoupled from static configurations and optimized for distributed environments.

* **Environment Variable Injection:** Hardcoded DNS routing must be replaced with OS-level environment variables. Update `main.py` and `consumer.py` to utilize `os.getenv()` for key endpoints, specifically `KAFKA_BROKER_URL` and `JAEGER_ENDPOINT_URL`.
* **Buffer Flush Enforcement:** To prevent the Uvicorn web server from holding messages in memory while waiting for arbitrary batch limits, explicit flush commands must be implemented. Modify the FastAPI producer to invoke `producer.flush(10.0)` instead of `producer.poll(0)`.
* **Delivery Diagnostics:** Network errors (e.g., `UNKNOWN_TOPIC`) must not be swallowed by Python's standard output buffers. Implement a `delivery_report` callback on the Kafka producer to capture network exceptions and inject these logs directly into the FastAPI HTTP return payload for immediate visibility in the UI.

## 2. Containerization & Cluster Provisioning

The following procedures detail the configuration of the local Kubernetes hypervisor and immutable image builds.

* **Dockerfile Configuration:** Ensure the build context includes an explicit `COPY . .` directive. All Python scripts and the `index.html` file must be physically packaged into the immutable image, as dynamic local volume mounts (`volumes: - .:/app`) are incompatible with standard Kubernetes deployments.
* **Cluster Initialization:** Bootstrap the local cluster (e.g., Minikube) using the Docker driver:
```bash
minikube start --driver=docker

```


* **Native Image Build:** To bypass external registry pull limitations, route the local terminal to the cluster's internal Docker daemon and build the application image natively:
```bash
eval $(minikube docker-env)
docker build -t enterprise-telemetry-app:v2 .

```



## 3. Stateful Infrastructure Deployment

Databases and observability backends must be provisioned and verified prior to deploying stateless application workloads.

* **Jaeger Observability Configuration:** Deploy the `jaegertracing/all-in-one` image via a standard `Deployment`. Expose the application via a `NodePort` service, mapping Port 4317 for internal gRPC telemetry ingestion and Port 16686 for the external user interface.
* **Kafka Broker Provisioning:** Deploy the official `apache/kafka:latest` image via a `StatefulSet`.
* **Single-Node KRaft Configuration:** For single-node environments operating without Zookeeper, strict replica overrides must be applied to prevent the consumer from failing to commit read-histories. Apply the following environment variables to the Kafka pod:
* `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1"`
* `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="1"`


* **Headless Service DNS:** The Kafka Service must be configured as Headless (`clusterIP: None`). This ensures the KRaft controller quorum can accurately resolve the exact StatefulSet pod name (`kafka-0.kafka.default.svc.cluster.local`).

## 4. Stateless Workload Deployment

Deploy the Python applications as isolated, scalable entities utilizing the locally built `enterprise-telemetry-app:v2` image.

* **InitContainer Sequencing:** To prevent `UNKNOWN_TOPIC` crash loops and stale connection caching, a `busybox` initialization container must be added to both the FastAPI and Consumer deployment manifests. Configure the InitContainer to verify Kafka's network availability before allowing the main application to boot:
```bash
until nc -z kafka 9092; do sleep 2; done

```


* **API Gateway Configuration:** Deploy `main.py` and expose it via a `NodePort` service mapping container port 8000 to the host network.
* **Background Worker Configuration:** Deploy `consumer.py` as an isolated `Deployment`. Do not attach an internal service, as the worker relies strictly on outbound connections. Explicitly override the startup command to `python -u consumer.py` to force unbuffered standard output.

## 5. Diagnostic & Troubleshooting Reference

Refer to the following operational behaviors when troubleshooting the distributed system:

* **Consumer Group Deadlocks (Phantom Consumers):** If logs fail to appear due to partition locking mechanisms (often caused by orphaned consumer processes), manually update the `group.id` in the consumer configuration. This forces Kafka to register the deployment as a net-new application and reassign the partition.
* **Offset Negotiation Amnesia:** Ensure `auto.offset.reset` is configured correctly for the environment. Relying on `"latest"` may cause the consumer to silently drop test messages submitted during the active partition negotiation window (typically 5–10 seconds upon startup).
* **Isolating Delivery Failures:** If messages fail to reach the consumer application, utilize native Kafka shell utilities directly inside the broker pod to isolate the failure point:
```bash
kubectl exec -it <kafka-pod-name> -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic-name>

```


*(If messages appear here, the failure lies within the Python Consumer pod. If empty, the failure lies at the Producer/Gateway ingress).*
