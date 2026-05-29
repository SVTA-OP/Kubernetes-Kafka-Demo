# Standard Operating Procedure (SOP): Docker to Kubernetes Conversion

## 1. Decoupling of Application into Components
### 1.1 Environment Variable Extraction
* Replace all hardcoded application variables (such as database credentials, API keys, and service URLs) with OS-level environment variables.
* Ensure these variables can be dynamically injected at runtime by the container orchestrator.

### 1.2 Memory State Management
* Flush and clear all active messages or transient states currently held in system memory.
* Gracefully drain running processes to prevent data loss during transition periods.

---

## 2. Containerization
### 2.1 Component Isolation
* Package each detached application component into its own dedicated container image (e.g., Apache Kafka, Jaeger Tracing, application microservices).
* Adhere to the single-responsibility principle for every containerized service.

### 2.2 Local Cluster Bootstrapping
* Initialize and boot up a local Kubernetes environment using **Minikube**.
* Configure Minikube to leverage the local Docker daemon as its container runtime provider (`eval $(minikube docker-env)`).

### 2.3 Application Image Construction
* Compile and build the core application Docker image locally.
* Ensure the image is tagged appropriately so it is immediately accessible within the local Minikube registry context.

---

## 3. Infrastructure Deployment
### 3.1 Distributed Tracing Setup
* Deploy the Jaeger tracing infrastructure using the standardized `jaeger-all-in-one` container image.
* Expose necessary ports for telemetry and UI visualization.

### 3.2 Message Broker Deployment
* Provision and deploy the Apache Kafka Broker instance to the local cluster.
* Configure persistent volumes if state retention is required across pod restarts.

### 3.3 Environment Configuration for Kafka
* Inject and map all required environment variables directly to the Kafka Pod configuration template.
* Verify connectivity and network routing parameters within the cluster namespace.

---

## 4. Workload Deployment
### 4.1 Application Workload Orchestration
* Deploy the Python application deployment resource into the cluster using the previously built local container image.
* Set the appropriate image pull policy to `Never` or `IfNotPresent` to enforce local image usage.

### 4.2 Isolated Consumer Workload
* Provision a separate, isolated Kubernetes Deployment specifically for the consumer component.
* Enforce unbuffered logging configurations by invoking the runtime script via `python -u` to guarantee real-time standard output (stdout) capture in the cluster logs.
