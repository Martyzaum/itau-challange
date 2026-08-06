#!/bin/bash
# Legacy starter-kit helper. This app only consumes financial *transaction* events
# (transaction + account.balance). Account-only payloads are invalid and go to DLT.
# Prefer: produce-transactions-events.sh / make kafka-produce-transactions-events
set -euo pipefail

echo "ERROR: produce-accounts-events.sh is not valid for this service." >&2
echo "This API consumes topic messages shaped as financial transactions:" >&2
echo '  {"transaction":{...},"account":{...,"balance":{...}}}' >&2
echo "" >&2
echo "Use instead:" >&2
echo "  make kafka-produce-transactions-events TOPIC=<topic> COUNT=<n>" >&2
echo "  # or: /redpanda-seed/produce-transactions-events.sh <topic> [count]" >&2
echo "" >&2
echo "For intentional poison/DLT drills, use: make chaos-poison-dlt" >&2
exit 1
