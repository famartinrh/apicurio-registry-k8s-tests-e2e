KIND_CLUSTER_NAME ?= "apicurio-cluster"

ifeq (1, $(shell command -v kind | wc -l))
KIND_CMD = kind
else
ifeq (1, $(shell command -v ./kind | wc -l))
KIND_CMD = ./kind
else
$(error "No kind binary found")
endif
endif

export E2E_SUITE_PROJECT_DIR=$(shell pwd)

# CI
run-operator-ci: kind-start kind-load-img pull-apicurio-registry run-operator-tests
run-ci: kind-start kind-load-img run-functional-tests

# testsuite dependencies
OPERATOR_METADATA_IMAGE?=docker.io/apicurio/apicurio-registry-operator-metadata:latest-dev
CATALOG_SOURCE_IMAGE=docker.io/apicurio/apicurio-registry-operator-catalog-source:latest-dev
export E2E_OLM_CATALOG_SOURCE_IMAGE=$(CATALOG_SOURCE_IMAGE)

BUNDLE_URL=https://raw.githubusercontent.com/Apicurio/apicurio-registry-operator/master/docs/resources/install.yaml
export E2E_OPERATOR_BUNDLE_PATH=$(BUNDLE_URL)

STRIMZI_BUNDLE_URL=https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.18.0/strimzi-cluster-operator-0.18.0.yaml
export E2E_STRIMZI_BUNDLE_PATH=$(STRIMZI_BUNDLE_URL)

# note there is no need to push CATALOG_SOURCE_IMAGE to docker hub
create-catalog-source-image:
	docker build -t $(CATALOG_SOURCE_IMAGE) --build-arg MANIFESTS_IMAGE=$(OPERATOR_METADATA_IMAGE) ./olm-catalog-source

kind-load-img: create-catalog-source-image
	${KIND_CMD} load docker-image $(CATALOG_SOURCE_IMAGE) --name $(KIND_CLUSTER_NAME) -v 5

kind-delete:
	${KIND_CMD} delete cluster --name ${KIND_CLUSTER_NAME}

kind-start:
ifeq (1, $(shell ${KIND_CMD} get clusters | grep ${KIND_CLUSTER_NAME} | wc -l))
	@echo "Cluster already exists" 
else
	@echo "Creating Cluster"	
	${KIND_CMD} create cluster --name ${KIND_CLUSTER_NAME} --config=./scripts/kind-config.yaml
	kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/master/deploy/static/provider/kind/deploy.yaml
	kubectl patch deployment ingress-nginx-controller -n ingress-nginx --type=json -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--enable-ssl-passthrough"}]'
	./scripts/setup-olm.sh
endif

run-all-tests:
	ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending -keepGoing \
		--cover --trace --race --progress -v

example-run-upgrade-tests:
	ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending -keepGoing \
		--cover --trace --race --progress -v ./testsuite/upgrade

run-operator-tests:
	ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending -keepGoing \
		--cover --trace --race --progress -v ./testsuite/bundle ./testsuite/olm -- -only-test-operator

run-functional-tests:
	ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending -keepGoing \
		--cover --trace --race --progress -v ./testsuite/bundle ./testsuite/olm

run-jpa-tests:
	ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending -keepGoing \
		--cover --trace --race --progress -v --focus="jpa" ./testsuite/bundle

example-run-jpa-and-streams-tests:
	ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending -keepGoing \
		--cover --trace --race --progress -v --focus="jpa|streams" -dryRun

example-run-jpa-with-olm-tests:
	ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending -keepGoing \
		--cover --trace --race --progress -v --focus="olm.*jpa" -dryRun

example-run-jpa-with-olm-and-upgrade-tests:
	ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending -keepGoing \
		--cover --trace --race --progress -v --focus="olm.*jpa|upgrade" -dryRun

# repo dependencies utilities
pull-apicurio-registry:
ifeq (,$(wildcard ./apicurio-registry))
	git clone git@github.com:Apicurio/apicurio-registry.git
else
	cd apicurio-registry; git pull
endif