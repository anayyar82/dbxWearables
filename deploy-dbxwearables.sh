#!/usr/bin/env bash
# deploy-dbxwearables.sh — One entry point to deploy the ZeroBus stack step by step.
#
# Delegates bundle orchestration to zeroBus/deploy.sh (infra → optional UC setup →
# readiness checks → app). Optionally runs the serverless bronze seed job afterward.
#
# Usage:
#   ./deploy-dbxwearables.sh --target dev --bootstrap
#   ./deploy-dbxwearables.sh --target dev --bootstrap --seed
#   ./deploy-dbxwearables.sh --target dev
#   ./deploy-dbxwearables.sh --target dev -- --skip-checks
#   ./deploy-dbxwearables.sh --target dev -- --validate
#
# First-time workspace flow:
#   1. ./deploy-dbxwearables.sh --target dev --bootstrap
#   2. Add the ZeroBus OAuth client secret to the secret scope (CLI message from deploy.sh).
#   3. ./deploy-dbxwearables.sh --target dev -- --skip-checks
#   4. Optional demo bronze rows: ./deploy-dbxwearables.sh --target dev --seed
#
# Medallion (silver/gold) Python lives under zeroBus/dbxW_zerobus_app/src/dlt/; wire it
# to a Lakeflow / Spark Declarative Pipeline in the workspace if not yet in the bundle.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZERO_BUS="${REPO_ROOT}/zeroBus"
DEPLOY_SCRIPT="${ZERO_BUS}/deploy.sh"
APP_BUNDLE="${ZERO_BUS}/dbxW_zerobus_app"

TARGET=""
BOOTSTRAP=false
RUN_SEED=false
DEPLOY_EXTRA=()

banner() {
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  $*"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

usage() {
  cat <<'EOF'
Usage: deploy-dbxwearables.sh --target <dev|hls_fde|prod> [options] [-- extra deploy.sh args]

Options:
  --bootstrap   First-time / full infra refresh: deploy infra, run wearables_uc_setup job,
                run readiness checks, deploy app bundle (same as zeroBus/deploy.sh --run-setup).
  --seed        After a successful deploy, run job seed_wearables_bronze_serverless (demo bronze).
  -h, --help    Show this help.

Anything after a lone "--" is passed through to zeroBus/deploy.sh, for example:
  --skip-checks   Deploy app even if the admin client secret is not in the scope yet
  --validate      Validate bundles only
  --destroy       Tear down deployed resources
  --infra         Infra bundle only
  --app           App bundle only (still runs readiness checks unless --skip-checks)

Examples:
  ./deploy-dbxwearables.sh --target dev --bootstrap
  ./deploy-dbxwearables.sh --target dev --bootstrap --seed
  ./deploy-dbxwearables.sh --target dev -- --skip-checks
EOF
}

die() { echo "Error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      [[ $# -ge 2 ]] || die "--target requires a value"
      TARGET="$2"
      shift 2
      ;;
    --bootstrap)
      BOOTSTRAP=true
      shift
      ;;
    --seed)
      RUN_SEED=true
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --)
      shift
      DEPLOY_EXTRA=("$@")
      break
      ;;
    *)
      die "Unknown option '$1'. Use --help."
      ;;
  esac
done

[[ -n "${TARGET}" ]] || die "--target is required."
[[ -f "${DEPLOY_SCRIPT}" ]] || die "Missing ${DEPLOY_SCRIPT} (expected ZeroBus deploy helper)."

command -v databricks &>/dev/null || die "Databricks CLI not found (https://docs.databricks.com/dev-tools/cli/install.html)"

banner "dbxWearables — full deploy"
echo "  Repository:  ${REPO_ROOT}"
echo "  Target:      ${TARGET}"
echo "  Bootstrap:   ${BOOTSTRAP}  (UC setup job + same order as zeroBus/deploy.sh --run-setup)"
echo "  Seed job:    ${RUN_SEED}"
if [[ ${#DEPLOY_EXTRA[@]} -gt 0 ]]; then
  echo "  Extra args:  ${DEPLOY_EXTRA[*]}"
fi

banner "Phase 1 — Prerequisites"
echo "  Using: $(command -v databricks)"
databricks version 2>/dev/null || true

banner "Phase 2 — Databricks Asset Bundles (zeroBus/deploy.sh)"
# macOS /bin/bash 3.2 + set -u: expanding "${empty_array[@]}" errors; append only when non-empty.
deploy_args=(--target "${TARGET}")
if [[ ${#DEPLOY_EXTRA[@]} -gt 0 ]]; then
  deploy_args+=("${DEPLOY_EXTRA[@]}")
fi
if [[ "${BOOTSTRAP}" == true ]]; then
  deploy_args+=(--run-setup)
fi

echo "  Command: ${DEPLOY_SCRIPT} ${deploy_args[*]}"
bash "${DEPLOY_SCRIPT}" "${deploy_args[@]}"

skip_seed=false
for a in "${deploy_args[@]}"; do
  if [[ "${a}" == "--validate" || "${a}" == "--destroy" ]]; then
    skip_seed=true
    break
  fi
done

if [[ "${RUN_SEED}" == true ]]; then
  banner "Phase 3 — Seed bronze (serverless job)"
  if [[ "${skip_seed}" == true ]]; then
    echo "  Skipped (--validate or --destroy was requested)."
  else
    [[ -d "${APP_BUNDLE}" ]] || die "App bundle directory missing: ${APP_BUNDLE}"
    echo "  Running: databricks bundle run seed_wearables_bronze_serverless --target ${TARGET}"
    (cd "${APP_BUNDLE}" && databricks bundle run seed_wearables_bronze_serverless --target "${TARGET}")
  fi
fi

banner "Finished"
echo "  Lakeview dashboard and AppKit app are defined in the app bundle."
echo "  Silver/gold DLT module: zeroBus/dbxW_zerobus_app/src/dlt/wearable_medallion.py"
echo "  (create or update a pipeline in the workspace if it is not bundle-managed yet)."
