#!/usr/bin/env bash
# Copy the Snowflake key into the Astro project so the Airflow container can read it.
# The copy is gitignored. Run this from the repository root before `astro dev start`.

set -euo pipefail

if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.example to .env first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

if [[ -z "${SNOWFLAKE_PRIVATE_KEY_PATH:-}" || ! -f "${SNOWFLAKE_PRIVATE_KEY_PATH}" ]]; then
  echo "SNOWFLAKE_PRIVATE_KEY_PATH in .env does not point at a key file." >&2
  exit 1
fi

cp "${SNOWFLAKE_PRIVATE_KEY_PATH}" include/rsa_key.p8
chmod 600 include/rsa_key.p8
echo "Copied Snowflake key to include/rsa_key.p8"
