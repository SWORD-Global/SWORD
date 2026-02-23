#!/bin/bash
set -e

PROJECT_ID="sword-qc"
BUCKET="sword-qc-data"
SERVICE="sword-reviewer"
REGION="us-central1"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE}"

echo "Building image..."
gcloud builds submit --tag "$IMAGE" --project "$PROJECT_ID"

echo "Deploying to Cloud Run..."
gcloud run deploy "$SERVICE" \
  --image "$IMAGE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --no-allow-unauthenticated \
  --memory 8Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 1 \
  --min-instances 0 \
  --port 8080 \
  --execution-environment gen2 \
  --add-volume name=gcs,type=cloud-storage,bucket="$BUCKET" \
  --add-volume-mount volume=gcs,mount-path=/mnt/gcs \
  --set-secrets PLANET_API_KEY=PLANET_API_KEY:latest \
  --set-env-vars "SWORD_DB_PATH=/tmp/sword/sword_v17c.duckdb,FIXES_DIR=/mnt/gcs/sword/lint_fixes,APP_FILE=lake_app.py"

echo "Done. URL:"
gcloud run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)'
