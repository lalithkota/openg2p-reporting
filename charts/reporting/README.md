# Reporting Framework

Helm chart for installing reporting module

## TL;DR

```console
$ helm repo add openg2p https://openg2p.github.io/openg2p-helm
$ helm install my-release openg2p/reporting
```

## Contents

This helm chart contains the following subcharts and they can be individually configured/installed/omitted, through the `values.yaml`.
- Bitnami's Kafka
- Debezium Kafka Connector
- Opensearch Kafka Connector
- A KafkaClient Pod for monitoring kafka topics, and making connect api calls

