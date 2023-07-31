#!/usr/bin/env bash

if [ $# -ge 1 ] ; then
  export KUBECONFIG=$1
fi

## Variables
NS=reporting
POSTGRES_NAMESPACE=postgres
POSTGRES_SECRET=postgres-postgresql
CHART_VERSION=12.0.2
INSTALL_NAME="openg2p"

helm repo add mosip https://mosip.github.io/mosip-helm
helm repo update

# Creating namespace with istio-injeciton
echo "Creating namespace"
kubectl create ns $NS

echo "Copying Postgres secret"
kubectl -n $NS delete --ignore-not-found=true secret $POSTGRES_SECRET
kubectl -n $POSTGRES_NAMESPACE get secret $POSTGRES_SECRET -o yaml | sed "s/namespace: $POSTGRES_NAMESPACE/namespace: $NS/g" | kubectl -n $NS create -f -

echo "Installing reporting helm"
helm -n $NS install reporting mosip/reporting -f values.yaml --wait --version $CHART_VERSION

echo "Waiting for helm chart to install"
sleep 30s

echo "Installing reporting-init helm"
DEBEZ_CONN_FILE="kafka-connect/debez-sample-conn.api"
ES_CONN_FOLDER="kafka-connect/es-connectors"

kubectl delete cm --ignore-not-found=true debz-conn-confmap -n $NS; kubectl create cm debz-conn-confmap --from-file=$DEBEZ_CONN_FILE -n $NS
kubectl delete cm --ignore-not-found=true es-conn-confmap -n $NS; kubectl create cm es-conn-confmap --from-file=$ES_CONN_FOLDER -n $NS
helm -n $NS install reporting-init mosip/reporting-init --wait --version $CHART_VERSION -f values-init.yaml --set base.db_prefix=$INSTALL_NAME --set debezium_connectors.existingConfigMap=debz-conn-confmap --set es_kafka_connectors.existingConfigMap=es-conn-confmap
