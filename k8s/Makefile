NAMESPACE ?= dev
IMAGE ?= my-app
VERSION ?= latest
DOCKER_USER ?= c1789

.PHONY: minikube-start
minikube-start:
	./start-minikube.sh $(NAMESPACE)

apply:
	@if [ "$(word 2, $(MAKECMDGOALS))" = "" ]; then \
		echo "❌ Usage: make apply <yaml-file>"; \
		exit 1; \
	fi; \
	FILE=$(word 2, $(MAKECMDGOALS)); \
	echo "🚀 Applying $$FILE"; \
	kubectl apply -f $$FILE

roll:
	@if [ "$(word 2,$(MAKECMDGOALS))" = "" ]; then \
		echo "❌ Usage: make roll <deployment-name>"; \
		exit 1; \
	fi; \
	DEPLOY=$(word 2,$(MAKECMDGOALS)); \
	echo "🔄 Restarting deployment '$$DEPLOY' in namespace observability"; \
	kubectl rollout restart deployment/$$DEPLOY -n observability; \
	echo "✅ Rollout done for '$$DEPLOY'"

# Prevent Make from treating the deployment name as a separate target
%:
	@: