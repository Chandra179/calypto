#!/usr/bin/env bash
set -e

NAMESPACE=${1:-dev}
MINIKUBE_BIN=$(which minikube)

# Color codes for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}✓${NC} $1"; }
log_warn() { echo -e "${YELLOW}⚠${NC} $1"; }
log_error() { echo -e "${RED}✗${NC} $1"; }

#############################################
# PHASE 0: PRE-FLIGHT CHECKS
#############################################
echo "🔍 Phase 0: Pre-flight checks..."

# Check if minikube is running
if ! minikube status >/dev/null 2>&1; then
  log_info "Starting Minikube..."
  minikube start
else
  log_info "Minikube already running"
fi

#############################################
# PHASE 1: INGRESS SETUP
#############################################
echo ""
echo "🌐 Phase 1: Setting up ingress..."

minikube addons enable ingress

log_info "Waiting for ingress controller deployment..."
kubectl wait --namespace ingress-nginx \
  --for=condition=available deployment/ingress-nginx-controller \
  --timeout=180s

log_info "Waiting for ingress controller pods..."
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=180s

# Critical: Wait for webhook to be fully initialized
log_info "Waiting for admission webhook to stabilize (30s)..."
sleep 30

#############################################
# PHASE 2: CREATE NAMESPACES
#############################################
echo ""
echo "📁 Phase 2: Creating namespaces..."
kubectl apply -f namespace.yaml
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -

#############################################
# PHASE 3: INSTALL/VERIFY ARGOCD
#############################################
echo ""
echo "🔧 Phase 3: Setting up ArgoCD..."

if kubectl get deployment argocd-server -n argocd &>/dev/null; then
  log_info "ArgoCD already installed"
else
  log_info "Installing ArgoCD..."
  kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
fi

log_info "Waiting for ArgoCD to be ready..."
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/name=argocd-server \
  -n argocd \
  --timeout=300s

# Wait for CRDs to be ready
sleep 15

# Ensure default project exists
if ! kubectl get appproject -n argocd default &>/dev/null; then
  log_warn "Creating default ArgoCD project..."
  cat <<EOF | kubectl apply -f -
apiVersion: argoproj.io/v1alpha1
kind: AppProject
metadata:
  name: default
  namespace: argocd
spec:
  description: Default project
  sourceRepos:
    - '*'
  destinations:
    - namespace: '*'
      server: '*'
  clusterResourceWhitelist:
    - group: '*'
      kind: '*'
EOF
  sleep 5
fi

#############################################
# PHASE 4: APPLY OBSERVABILITY STACK ONLY
#############################################
echo ""
echo "📊 Phase 4: Deploying observability stack..."

kubectl apply -f prometheus.yaml
kubectl apply -f grafana.yaml
kubectl apply -f alloy.yaml
kubectl apply -f loki.yaml
kubectl apply -f jaeger.yaml
kubectl apply -f app.yaml

log_info "Waiting for observability pods to be ready..."
kubectl wait --for=condition=ready pod -l app=prometheus -n observability --timeout=120s || log_warn "Prometheus not ready yet"
kubectl wait --for=condition=ready pod -l app=grafana -n observability --timeout=120s || log_warn "Grafana not ready yet"
kubectl wait --for=condition=ready pod -l app=loki -n observability --timeout=120s || log_warn "Loki not ready yet"

#############################################
# PHASE 5: APPLY INGRESS (BEFORE APP DEPLOYMENT)
#############################################
echo ""
echo "🌐 Phase 5: Configuring ingress..."

kubectl apply -f ingress.yaml

log_info "Ingress rules applied"

# Give ingress time to configure routes
log_info "Waiting for ingress to configure routes (15s)..."
sleep 10

#############################################
# PHASE 6: REGISTER ARGOCD APPLICATION
#############################################
echo ""
echo "🎯 Phase 6: Registering ArgoCD application..."

# Delete existing ArgoCD app if it exists to ensure clean state
kubectl delete application my-app -n argocd --ignore-not-found=true
sleep 5

# Apply ArgoCD application
kubectl apply -f argocd.yaml

log_info "Waiting for ArgoCD to sync and deploy application..."
sleep 30

# Wait for app pods to be ready (deployed by ArgoCD)
log_info "Waiting for app pods to be ready..."
kubectl wait --for=condition=ready pod -l app=my-app -n "$NAMESPACE" --timeout=180s || log_warn "App pods not ready yet"

# Verify replica count
REPLICA_COUNT=$(kubectl get deployment my-app -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
READY_REPLICAS=$(kubectl get deployment my-app -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")

echo ""
echo "📊 Deployment Status:"
echo "   Expected replicas: 3"
echo "   Current replicas: $REPLICA_COUNT"
echo "   Ready replicas: $READY_REPLICAS"

if [ "$REPLICA_COUNT" != "3" ]; then
  log_warn "Replica count mismatch! Expected 3, got $REPLICA_COUNT"
  log_warn "ArgoCD may still be syncing..."
fi

#############################################
# PHASE 7: START MINIKUBE TUNNEL
#############################################
echo ""
echo "🔗 Phase 7: Starting minikube tunnel..."

# Check if tunnel is already running
if pgrep -f "minikube tunnel" > /dev/null; then
  log_info "Minikube tunnel already running"
else
  log_warn "Starting minikube tunnel in new terminal..."
  gnome-terminal -- bash -c "echo 'Starting Minikube Tunnel - Keep this window open!'; echo ''; sudo -E $MINIKUBE_BIN tunnel; exec bash" 2>/dev/null || {
    log_error "Could not open terminal. Run this manually in another terminal:"
    echo ""
    echo "   sudo minikube tunnel"
    echo ""
    read -p "Press ENTER after starting minikube tunnel manually..."
  }
  
  # Wait for tunnel to establish
  log_info "Waiting for tunnel to establish (7s)..."
  sleep 7
fi

#############################################
# PHASE 8: VERIFY HOSTS FILE
#############################################
echo ""
echo "📝 Phase 8: Verifying /etc/hosts..."
MINIKUBE_IP=$(minikube ip)
echo "   Minikube IP: $MINIKUBE_IP"

REQUIRED_HOSTS=("grafana.local" "prometheus.local" "jaeger.local" "alloy.local" "app.local")
MISSING_HOSTS=()

for host in "${REQUIRED_HOSTS[@]}"; do
  if ! grep -q "$host" /etc/hosts 2>/dev/null; then
    MISSING_HOSTS+=("$host")
  fi
done

if [ ${#MISSING_HOSTS[@]} -gt 0 ]; then
  log_warn "Missing hosts entries detected!"
  echo ""
  echo "   Run this command to add them:"
  echo ""
  echo "   sudo bash -c 'cat >> /etc/hosts << EOL"
  for host in "${MISSING_HOSTS[@]}"; do
    echo "$MINIKUBE_IP $host"
  done
  echo "EOL'"
  echo ""
  read -p "Press ENTER after updating /etc/hosts..."
else
  log_info "All required hosts entries present"
fi

#############################################
# PHASE 9: PORT FORWARDING
#############################################
echo ""
echo "🔌 Phase 9: Setting up port-forwards..."

# Kill existing port-forwards
pkill -f "kubectl port-forward.*$NAMESPACE.*8080" 2>/dev/null || true
pkill -f "kubectl port-forward.*argocd.*8081" 2>/dev/null || true
sleep 2

# Start port-forwards
log_info "Starting app port-forward..."
nohup kubectl port-forward svc/my-app -n "$NAMESPACE" 8080:8080 >/tmp/my-app.log 2>&1 &
APP_PF_PID=$!

log_info "Starting ArgoCD port-forward..."
nohup kubectl port-forward svc/argocd-server -n argocd 8081:443 >/tmp/argocd.log 2>&1 &
ARGOCD_PF_PID=$!

# Verify port-forwards are working
sleep 3
if ps -p $APP_PF_PID > /dev/null; then
  log_info "App port-forward running (PID: $APP_PF_PID)"
else
  log_warn "App port-forward may have failed. Check /tmp/my-app.log"
fi

if ps -p $ARGOCD_PF_PID > /dev/null; then
  log_info "ArgoCD port-forward running (PID: $ARGOCD_PF_PID)"
else
  log_warn "ArgoCD port-forward may have failed. Check /tmp/argocd.log"
fi

#############################################
# PHASE 10: FINAL HEALTH CHECKS
#############################################
echo ""
echo "🏥 Phase 10: Running final health checks..."

# Wait for everything to be truly ready
log_info "Waiting for all services to be fully ready (20s)..."
sleep 15

# Check pod status
echo ""
echo "📊 Pod Status:"
kubectl get pods -n "$NAMESPACE" --no-headers | awk '{print "   " $1 ": " $3}'
kubectl get pods -n observability --no-headers | awk '{print "   " $1 ": " $3}'

# Check ArgoCD application status
echo ""
echo "🎯 ArgoCD Application Status:"
kubectl get application my-app -n argocd -o jsonpath='{.status.sync.status}' 2>/dev/null && echo "" || echo "   Not available"

# Check ingress status
echo ""
echo "🌐 Ingress Status:"
kubectl get ingress -A -o wide | tail -n +2 | awk '{print "   " $2 " (" $1 "): " $4}' || echo "   No ingress found"

#############################################
# PHASE 11: GET ARGOCD PASSWORD
#############################################
echo ""
echo "🔑 ArgoCD Credentials:"
echo "   Username: admin"
echo -n "   Password: "
ARGOCD_PASSWORD=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" 2>/dev/null | base64 -d)
if [ -n "$ARGOCD_PASSWORD" ]; then
  echo "$ARGOCD_PASSWORD"
else
  echo "(not available yet)"
fi
echo ""

#############################################
# PHASE 12: OPEN BROWSERS (WITH CONFIRMATION)
#############################################
echo ""
echo "🌐 Phase 12: Opening services in browser..."
echo ""
read -p "Open services in browser now? (y/n) " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
  log_info "Opening services (staggered to avoid overwhelming the system)..."
  
  xdg-open http://grafana.local 2>/dev/null &
  sleep 2
  xdg-open http://prometheus.local 2>/dev/null &
  sleep 2
  xdg-open http://jaeger.local 2>/dev/null &
  sleep 2
  xdg-open http://alloy.local 2>/dev/null &
  sleep 2
  xdg-open http://app.local 2>/dev/null &
  sleep 2
  xdg-open https://localhost:8081 2>/dev/null &
  
  log_info "Browsers opened."
else
  log_info "Skipped opening browsers. Access services manually when ready."
fi

#############################################
# SUMMARY
#############################################
echo ""
echo "✅ ========================================="
echo "✅ DEPLOYMENT COMPLETE!"
echo "✅ ========================================="
echo ""
echo "📋 Service URLs:"
echo "   • Grafana:    http://grafana.local (admin/admin)"
echo "   • Prometheus: http://prometheus.local"
echo "   • Jaeger:     http://jaeger.local"
echo "   • Alloy:      http://alloy.local"
echo "   • App:        http://app.local"
echo "   • ArgoCD:     https://localhost:8081 (admin/$ARGOCD_PASSWORD)"
echo ""
echo "🎯 ArgoCD is managing your app deployment!"
echo "   • View sync status: kubectl get application my-app -n argocd"
echo ""
echo "🔍 Quick Status Commands:"
echo "   kubectl get pods -n $NAMESPACE"
echo "   kubectl get deployment my-app -n $NAMESPACE"
echo "   kubectl get application my-app -n argocd"
echo "   kubectl logs -n $NAMESPACE -l app=my-app"
echo ""
echo "⚠️  IMPORTANT:"
echo "   If you see replica count > 3, run:"
echo "   kubectl delete application my-app -n argocd"
echo "   kubectl delete deployment my-app -n dev"
echo "   Then re-run this script"
echo ""