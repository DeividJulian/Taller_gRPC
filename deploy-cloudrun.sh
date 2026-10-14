#!/usr/bin/env bash
# Despliegue en Google Cloud Run con HTTP/2 de extremo a extremo
# (necesario para que el streaming bidireccional funcione en la nube).
#
# Uso:  ./deploy-cloudrun.sh TU_PROJECT_ID [region]
set -euo pipefail

PROJECT_ID="${1:?Indica el PROJECT_ID de Google Cloud}"
REGION="${2:-us-central1}"
SERVICE="grpc-chat"

gcloud config set project "$PROJECT_ID"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com

gcloud run deploy "$SERVICE" \
  --source . \
  --region "$REGION" \
  --allow-unauthenticated \
  --use-http2 \
  --max-instances 1 \
  --session-affinity \
  --timeout 3600 \
  --memory 512Mi

URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format 'value(status.url)')
echo ""
echo "✅ Desplegado en: $URL"
echo "   Web:  $URL"
echo "   CLI:  npm run cli -- --url $URL --name TuNombre"
echo "   Test: CHAT_URL=$URL npm run test:e2e"
