# Reporting Framework Helm Chart

Helm chart for installing reporting framework (infra for reporting).

This chart contains the following components:

- Debezium Kafka Connector
- Opensearch Kafka Connector

Note: Reporting Charts versions lower than 1.3 install kafka and opensearch dependencies. Helm charts version 1.3.x and higher do NOT install the dependencies along with them.
