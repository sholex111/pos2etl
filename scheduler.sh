#!/bin/sh

# Set the interval for the ETL run (e.g., run every 1 minute)
# This script will run forever, sleeping between runs.
INTERVAL=60

while true; do
  echo "--- $(date) - Starting ETL run ---"

  # Use 'docker-compose run' to execute the etl_service script once.
  # The --rm flag ensures the container is removed immediately after execution.
  # The etl_service will update the database based on new CSVs.
  docker-compose run --rm etl_service

  echo "--- $(date) - ETL run finished. Sleeping for $INTERVAL seconds ---"
  sleep $INTERVAL
done