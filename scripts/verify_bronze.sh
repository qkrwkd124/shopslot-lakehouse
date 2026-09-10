#!/usr/bin/env bash
set -euo pipefail
root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root_dir}"
# This is a P0-sized reconciliation, not a production full-history scan.
docker compose exec -T mysql sh -ec \
  'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -N "$MYSQL_DATABASE" -e "SELECT event_id FROM outbox_events ORDER BY event_id"' \
  | docker compose exec -T spark sh -ec '
      ids_file=$(mktemp /tmp/shopslot-outbox-ids.XXXXXX)
      trap '\''rm -f "$ids_file"'\'' EXIT
      cat > "$ids_file"
      python3 /opt/spark/work-dir/spark/run.py submit \
        /opt/spark/work-dir/spark/jobs/verify_bronze.py --outbox-ids-file "$ids_file"
    '
