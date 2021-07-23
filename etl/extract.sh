#!/bin/bash

DATE_START=$1
DATE_END=$2

TIMESTAMP=$(date +%s)

# Activate the pex environment
source /home/jpgravel/.pexenv/bin/activate
python --version >&2
echo \### Extraction timestamp: $TIMESTAMP \### >&2

echo \### Extraction of recorded data \### >&2
cat data/decol-tags-recorded-2m.txt | \
  python -m pex.extract -tRecorded -a$DATE_START -e$DATE_END -i2m -b27000 | \
  hdfs dfs -put - /rawdata/environment/decolmatage/recorded-2m-$TIMESTAMP.csv
  
echo \### Extraction of avarged values \### >&2
cat data/decol-tags-avg-1h.txt | \
  python -m pex.extract -tSummary -a$DATE_START -e$DATE_END -i1h | \
  hdfs dfs -put - /rawdata/environment/decolmatage/average-1h-$TIMESTAMP.csv

echo \### Extraction of interpolated values \### >&2
cat data/decol-tags-avg-1h.txt | \
  python -m pex.extract -a$DATE_START -e$DATE_END -i1s | \
  hdfs dfs -put - /rawdata/environment/decolmatage/interpolated-1s-$TIMESTAMP.csv
