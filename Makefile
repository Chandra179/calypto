ins:
	go mod tidy && go mod vendor

up:
	docker compose up -d

build:
	docker compose up --build -d

run:
	go run cmd/myapp/main.go

swag:
	swag init -g /cmd/myapp/main.go -o api

IMAGE ?= my-app
VERSION ?= latest
DOCKER_USER ?= c1789

.PHONY: docker-push
docker-push:
	docker build -t $(IMAGE):$(VERSION) .
	docker tag $(IMAGE):$(VERSION) $(DOCKER_USER)/$(IMAGE):$(VERSION)
	docker push $(DOCKER_USER)/$(IMAGE):$(VERSION)

.PHONY: add-secrets
add-secrets:
	./add-secrets.sh
