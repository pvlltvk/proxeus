#!/usr/bin/env bash
# api_smoke.sh — exercise promxy's Prometheus HTTP API surface and assert each
# response carries the expected envelope shape. Intended for live verification
# against a running promxy (typically the dev sandbox), not as part of `go test`.
#
# Usage:
#   ./scripts/api_smoke.sh                    # defaults to http://localhost:18082
#   PROMXY_URL=http://promxy:8082 ./scripts/api_smoke.sh
#
# Requires: curl, jq. Exits non-zero on the first failed assertion.

set -euo pipefail

PROMXY_URL="${PROMXY_URL:-http://localhost:18082}"

red()   { printf '\033[31m%s\033[0m\n' "$*" >&2; }
green() { printf '\033[32m%s\033[0m\n'  "$*"; }
yel()   { printf '\033[33m%s\033[0m\n'  "$*"; }

fail=0
section() { yel "── $*"; }
ok()      { green "  PASS  $*"; }
bad()     { red "  FAIL  $*"; fail=$((fail + 1)); }

# --- helpers ---------------------------------------------------------------

# fetch URL, store body in $RESP_BODY and status code in $RESP_CODE.
fetch() {
    local path="$1"
    RESP_BODY=$(curl -fsS --max-time 10 -w "\n%{http_code}" "${PROMXY_URL}${path}" 2>/dev/null || true)
    RESP_CODE="${RESP_BODY##*$'\n'}"
    RESP_BODY="${RESP_BODY%$'\n'*}"
}

assert_200() {
    if [[ "${RESP_CODE}" != "200" ]]; then
        bad "$1 → HTTP ${RESP_CODE} (want 200)"
        return 1
    fi
}

assert_jq() {
    # $1 = label, $2 = jq filter, $3 = expected
    local got
    got=$(printf '%s' "${RESP_BODY}" | jq -r "$2" 2>/dev/null || echo "<jq-error>")
    if [[ "${got}" != "$3" ]]; then
        bad "$1 → \`$2\` = '${got}', want '$3'"
        return 1
    fi
    return 0
}

# --- /api/v1/status/buildinfo ---------------------------------------------
section "/api/v1/status/buildinfo (Grafana health probe target)"
fetch "/api/v1/status/buildinfo"
assert_200 buildinfo && \
    assert_jq buildinfo '.status' "success" && \
    assert_jq buildinfo '.data | has("version")' "true" && \
    ok "buildinfo: envelope + version present"

# --- /api/v1/status/walreplay (F5) ----------------------------------------
section "/api/v1/status/walreplay (F5 — no WAL, return success/zeros)"
fetch "/api/v1/status/walreplay"
assert_200 walreplay && \
    assert_jq walreplay '.status' "success" && \
    assert_jq walreplay '.data.min'     "0" && \
    assert_jq walreplay '.data.max'     "0" && \
    assert_jq walreplay '.data.current' "0" && \
    ok "walreplay: envelope + min/max/current=0"

# --- /api/v1/status/flags (F6) --------------------------------------------
section "/api/v1/status/flags (F6 — empty data object, no Go struct names)"
fetch "/api/v1/status/flags"
assert_200 flags && \
    assert_jq flags '.status' "success" && \
    assert_jq flags '.data | type'   "object" && \
    assert_jq flags '.data | length' "0" && \
    ok "flags: envelope + empty object"

# --- /api/v1/status/runtimeinfo -------------------------------------------
section "/api/v1/status/runtimeinfo"
fetch "/api/v1/status/runtimeinfo"
if [[ "${RESP_CODE}" == "200" ]]; then
    assert_jq runtimeinfo '.status' "success" && \
        ok "runtimeinfo: envelope present"
else
    yel "  SKIP  runtimeinfo not exposed (HTTP ${RESP_CODE})"
fi

# --- /api/v1/status/config ------------------------------------------------
section "/api/v1/status/config"
fetch "/api/v1/status/config"
assert_200 config && \
    assert_jq config '.status' "success" && \
    assert_jq config '.data | has("yaml")' "true" && \
    ok "config: envelope + yaml field"

# --- /api/v1/labels --------------------------------------------------------
section "/api/v1/labels"
fetch "/api/v1/labels"
assert_200 labels && \
    assert_jq labels '.status' "success" && \
    assert_jq labels '.data | type' "array" && \
    ok "labels: envelope + array"

# --- /api/v1/label/__name__/values ----------------------------------------
section "/api/v1/label/__name__/values"
fetch "/api/v1/label/__name__/values"
assert_200 labelvalues && \
    assert_jq labelvalues '.status' "success" && \
    assert_jq labelvalues '.data | type' "array" && \
    ok "label __name__ values: envelope + array"

# --- /api/v1/series (F2) ---------------------------------------------------
section "/api/v1/series (F2 — cross-backend dedup)"
fetch "/api/v1/series?match%5B%5D=up"
assert_200 series && \
    assert_jq series '.status' "success" && \
    assert_jq series '.data | type' "array" && \
    ok "series: envelope + array (n=$(printf '%s' "${RESP_BODY}" | jq '.data | length'))"

# --- /api/v1/query ---------------------------------------------------------
section "/api/v1/query"
fetch "/api/v1/query?query=up"
assert_200 query && \
    assert_jq query '.status' "success" && \
    assert_jq query '.data.resultType' "vector" && \
    ok "query: envelope + resultType=vector"

# --- /api/v1/query_range ---------------------------------------------------
section "/api/v1/query_range"
now=$(date +%s)
start=$((now - 300))
fetch "/api/v1/query_range?query=up&start=${start}&end=${now}&step=60"
assert_200 query_range && \
    assert_jq query_range '.status' "success" && \
    assert_jq query_range '.data.resultType' "matrix" && \
    ok "query_range: envelope + resultType=matrix"

# --- /api/v1/metadata ------------------------------------------------------
section "/api/v1/metadata"
fetch "/api/v1/metadata"
assert_200 metadata && \
    assert_jq metadata '.status' "success" && \
    ok "metadata: envelope present"

# --- /metrics --------------------------------------------------------------
section "/metrics (promxy's own scrape endpoint)"
fetch "/metrics"
assert_200 metrics && \
    grep -q '^promxy_cross_group_dedup_collisions_total' <<<"${RESP_BODY}" && \
    grep -q '^promxy_cross_group_dedup_metadata_collisions_total' <<<"${RESP_BODY}" && \
    ok "metrics: B1 + F2 collision counters exposed"

# --- summary ---------------------------------------------------------------
echo
if (( fail == 0 )); then
    green "All API smoke checks passed against ${PROMXY_URL}"
    exit 0
else
    red "${fail} check(s) failed against ${PROMXY_URL}"
    exit 1
fi
