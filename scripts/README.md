# OpenG2P Reporting Instructions

## Prerequisites

The following prerequisites need to be deployed and running on the k8s cluster.

- [Elasticsearch](https://github.com/openg2p/openg2p-deployment/tree/1.1.0/kubernetes/logging) in logging.
- [Kafka](https://github.com/openg2p/openg2p-deployment/tree/1.1.0/kubernetes/kafka).
- [PostgreSQL](https://github.com/openg2p/openg2p-deployment/tree/1.1.0/kubernetes/postgresql).
- [OpenG2P](https://github.com/openg2p/openg2p-deployment/tree/1.1.0/kubernetes/openg2p).

## Installation

- Run the following to install reporting
  ```sh
  ./install.sh
  ```
- Import the dashboards present in [dashboards](../dashboards) folder.
  - Navigate to Kibana Stack Management -> Kibana Section -> Saved Objects.
  - Import all files in dashboards folder.
