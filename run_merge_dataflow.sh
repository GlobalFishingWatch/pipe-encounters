#!/usr/bin/env bash
#
# Runs the merge_encounters pipeline on Dataflow via docker-compose.
#
# Prereqs:
#   - docker + docker compose installed
#   - GCP auth volume populated once:  make docker-gcp
#     (creates the "gcp" volume and runs `gcloud auth application-default login`)
#   - The dev image is built:          make docker-build
#
# Usage:
#   ./run_merge_dataflow.sh
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Job configuration
# ---------------------------------------------------------------------------
PROJECT="world-fishing-827"
REGION="us-central1"

# Service account the Dataflow workers run as (must have BQ/GCS access).
SERVICE_ACCOUNT_EMAIL="research-and-development@world-fishing-827.iam.gserviceaccount.com"

# Dataflow job name: must match [a-z]([-a-z0-9]*[a-z0-9])? (no underscores/uppercase).
# Include a stamp so re-runs don't collide. Pass one in as $1 to override.
JOB_NAME="${1:-encounters-merge-quick-fix-3}"

# GCS staging/temp bucket used by the BigQuery read export and load steps.
TEMP_LOCATION="gs://world-fishing-827-us-central1-dataflow/ttl7/temp"

# Data tables / date range (from the requested configuration).
START_DATE="2012-01-01"
END_DATE="2026-01-01"
RAW_TABLE="gfw-int-ais-datalake.encounters_v1.raw_encounters"
SINK_TABLE="world-fishing-827.vi_928_quick_fix_3.encounters"
VESSEL_ID_TABLE="world-fishing-827.prj_entity_hull.entity_epoch_v20260801"
SPATIAL_MEASURES_TABLE="global-fishing-watch.pipe_static.spatial_measures_clustered_v20260403"

# Worker sizing for a multi-year backfill.
MAX_NUM_WORKERS=50
DISK_SIZE_GB=100

# Prebuild config. Dataflow builds a custom SDK worker container (base image +
# this package) on Cloud Build and pushes it to Artifact Registry.
#
# BASE_SDK_IMAGE must be FULLY-QUALIFIED (docker.io/...). Beam writes it verbatim
# as the `FROM` in the generated Dockerfile; Cloud Build's buildah enforces
# short-name resolution and cannot prompt without a TTY, so a bare
# `apache/beam_...` name fails with exit code 125.
BASE_SDK_IMAGE="docker.io/apache/beam_python3.12_sdk:2.71.0"
DOCKER_REGISTRY_PUSH_URL="us-central1-docker.pkg.dev/world-fishing-827/development/pipe-encounters-vi-928"

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------
docker compose run --rm merge_encounters \
  --start_date="${START_DATE}" \
  --end_date="${END_DATE}" \
  --raw_table="${RAW_TABLE}" \
  --sink_table="${SINK_TABLE}" \
  --vessel_id_table="${VESSEL_ID_TABLE}" \
  --spatial_measures_table="${SPATIAL_MEASURES_TABLE}" \
  --labels=project=quick-fix-3 \
  --labels=mode=backfill \
  --labels=stage=encounters--v4 \
  --runner=DataflowRunner \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --service_account_email="${SERVICE_ACCOUNT_EMAIL}" \
  --job_name="${JOB_NAME}" \
  --temp_location="${TEMP_LOCATION}" \
  --max_num_workers="${MAX_NUM_WORKERS}" \
  --disk_size_gb="${DISK_SIZE_GB}" \
  --setup_file=./setup.py \
  --requirements_file=requirements.txt \
  --prebuild_sdk_container_engine=cloud_build \
  --docker_registry_push_url="${DOCKER_REGISTRY_PUSH_URL}" \
  --sdk_container_image="${BASE_SDK_IMAGE}" \
  --sdk_location=container
