#!/usr/bin/env bash
set -e

NAMESPACE=${1:-dev}

# Load .env file from project root
if [[ -f ".env" ]]; then
  echo "Loading .env file..."
  export $(grep -v '^#' .env | xargs)
else
  echo "[ERROR] .env file not found in current directory."
  exit 1
fi

# Ensure namespace exists
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

echo "Creating Kubernetes secret from .env values..."
kubectl create secret generic app-secrets \
  --namespace="$NAMESPACE" \
  --from-literal=GOOGLE_CLIENT_ID="${GOOGLE_CLIENT_ID}" \
  --from-literal=GOOGLE_CLIENT_SECRET="${GOOGLE_CLIENT_SECRET}" \
  --from-literal=GOOGLE_REDIRECT_URL="${GOOGLE_REDIRECT_URL}" \
  --from-literal=GITHUB_CLIENT_ID="${GITHUB_CLIENT_ID}" \
  --from-literal=GITHUB_CLIENT_SECRET="${GITHUB_CLIENT_SECRET}" \
  --from-literal=GITHUB_REDIRECT_URL="${GITHUB_REDIRECT_URL}" \
  --from-literal=JWT_SECRET="${JWT_SECRET}" \
  --from-literal=JWT_EXPIRATION="${JWT_EXPIRATION}" \
  --from-literal=STATE_TIMEOUT="${STATE_TIMEOUT}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secrets applied successfully in namespace $NAMESPACE!"
