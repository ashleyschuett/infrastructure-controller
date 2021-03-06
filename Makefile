SHELL=/bin/bash
PROJECT_NAME := "infrastructure-controller"
IMAGE_TAG ?= "latest"
PKG := "github.com/containership/$(PROJECT_NAME)"
PKG_LIST := $(shell go list ./...)
GO_FILES := $(shell find . -type f -not -path './vendor/*' -name '*.go')

.PHONY: all
all: build ## (default) Build

.PHONY: fmt-check
fmt-check: ## Check the file format
	@gofmt -s -e -d $(GO_FILES) | read; \
		if [ $$? == 0 ]; then \
			echo "gofmt check failed:"; \
			gofmt -s -e -d $(GO_FILES); \
			exit 1; \
		fi

.PHONY: lint
lint: ## Lint the files
	@golint -set_exit_status ${PKG_LIST}

.PHONY: test
test: ## Run unit tests
	@go test -short ${PKG_LIST}

.PHONY: coverage
coverage: ## Run unit tests with coverage checking / codecov integration
	@go test -short -coverprofile=coverage.txt -covermode=count ${PKG_LIST}

.PHONY: vet
vet: ## Vet the files
	@go vet ${PKG_LIST}

## Read about data race https://golang.org/doc/articles/race_detector.html
## to not test file for race use `// +build !race` at top
.PHONY: race
race: ## Run data race detector
	@go test -race -short ${PKG_LIST}

.PHONY: msan
msan: ## Run memory sanitizer (only works on linux/amd64)
	@go test -msan -short ${PKG_LIST}

.PHONY: help
help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: dep
dep:
	@dep ensure

.PHONY: build
build: ## Build the controller in Docker
	@docker image build -t containership/$(PROJECT_NAME):$(IMAGE_TAG) . \
		--build-arg GIT_DESCRIBE=`git describe --dirty` \
		--build-arg GIT_COMMIT=`git rev-parse --short HEAD` \

.PHONY: release
release: ## Build release image for controller (must be on semver tag)
	@./hack/build-release.sh

### Commands for local development
.PHONY: deploy
deploy: ## Deploy the controller
	kubectl apply -f deploy/development/infrastructure-controller.yaml

.PHONY: undeploy
undeploy: ## Delete the controller
	kubectl delete --now -f deploy/development/infrastructure-controller.yaml
