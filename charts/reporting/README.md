# Reporting Framework

Helm chart for installing reporting module

## Manual Installation

Reporting framework can be installed directly as part of OpenG2P modules, like Social Registry and PBMS, etc.

```console
$ helm repo add openg2p https://openg2p.github.io/openg2p-helm
$ helm install reporting openg2p/reporting
```

## Contents

This helm chart contains the following subcharts and they can be individually configured/installed/omitted, through the `values.yaml`.
- Kafka + Kafka UI
- OpenSearch
- Debezium Kafka Connector
- Opensearch Kafka Connector
