#!/bin/sh
set -o errexit

reg_name='kind-registry'
reg_port='5000'

# connect the registry to the cluster network
docker network connect "kind" "${reg_name}" || true

# # tell https://tilt.dev to use the registry
# # https://docs.tilt.dev/choosing_clusters.html#discovering-the-registry
# for node in $(./kind get nodes --name apicurio-cluster); do
#   kubectl annotate node "${node}" "kind.x-k8s.io/registry=localhost:${reg_port}";
# done

# Document the local registry
# https://github.com/kubernetes/enhancements/tree/master/keps/sig-cluster-lifecycle/generic/1755-communicating-a-local-registry
# NOTE: use ${reg_name}:${reg_port} as the host instead of the localhost:${reg_port} 
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "${reg_name}:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF