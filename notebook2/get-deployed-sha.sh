#!/bin/bash
set -ex

kubectl -n default get --selector=app=notebook2 deployments -o "jsonpath={.items[*].metadata.labels.hail\.is/sha}"
