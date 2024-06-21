FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

ARG DEBEZIUM_VERSION=2.6.2.Final

COPY kafka-connect-transforms /kafka-connect-transforms
RUN cd /kafka-connect-transforms && \
    mvn install -DskipTests && \
    cd target && \
    wget https://repo.maven.apache.org/maven2/io/debezium/debezium-core/${DEBEZIUM_VERSION}/debezium-core-${DEBEZIUM_VERSION}.jar && \
    wget https://repo.maven.apache.org/maven2/io/debezium/debezium-api/${DEBEZIUM_VERSION}/debezium-api-${DEBEZIUM_VERSION}.jar


FROM apache/kafka:3.7.0 AS runner
ARG KAFKA_VERSION=3.7.0

WORKDIR /opt/kafka
RUN mkdir connectors

ARG OPENSEARCH_CONNECTOR_VERSION=3.1.1

RUN cd connectors && \
    wget https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v${OPENSEARCH_CONNECTOR_VERSION}/opensearch-connector-for-apache-kafka-${OPENSEARCH_CONNECTOR_VERSION}.zip && \
    unzip opensearch-connector-for-apache-kafka-${OPENSEARCH_CONNECTOR_VERSION}.zip -d opensearch && \
    mv opensearch/opensearch-connector-for-apache-kafka-${OPENSEARCH_CONNECTOR_VERSION} opensearch-connector-for-apache-kafka && \
    rm -r opensearch *.zip

COPY connect-distributed.properties config/

COPY --from=builder /kafka-connect-transforms/target/kafka-connect-transforms-*.jar /opt/kafka/connectors/kafka-connect-transforms/
COPY --from=builder /kafka-connect-transforms/target/debezium-*.jar /opt/kafka/connectors/kafka-connect-transforms/

EXPOSE 8083

CMD ["bin/connect-distributed.sh" , "config/connect-distributed.properties"]
