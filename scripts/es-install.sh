#!/usr/bin/env bash

if [ $# -ge 1 ] ; then
  export KUBECONFIG=$1
fi

## Variables
NS=reporting

# Add helm repos
echo "Adding helm repos"
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

echo "Creating namespace"
kubectl create ns $NS

kubectl -n $NS apply -f virtualservice.yaml

helm -n $NS install elasticsearch bitnami/elasticsearch --wait -f es-values.yaml
