ARG KAFKA_VERSION=3.7.0
FROM apache/kafka:${KAFKA_VERSION}

WORKDIR /opt/kafka
RUN mkdir connectors

ARG OPENSEARCH_CONNECTOR_VERSION=3.1.1

RUN cd connectors && \
    wget https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v${OPENSEARCH_CONNECTOR_VERSION}/opensearch-connector-for-apache-kafka-${OPENSEARCH_CONNECTOR_VERSION}.zip && \
    unzip opensearch-connector-for-apache-kafka-${OPENSEARCH_CONNECTOR_VERSION}.zip -d opensearch && \
    mv opensearch/opensearch-connector-for-apache-kafka-${OPENSEARCH_CONNECTOR_VERSION} opensearch-connector-for-apache-kafka && \
    rm -r opensearch *.zip

COPY connect-distributed.properties config/

EXPOSE 8083

CMD ["bin/connect-distributed.sh" , "config/connect-distributed.properties"]

