#!/bin/bash
spark-submit \
     --master yarn \
     --deploy-mode client \
     --driver-memory 5g \
     --executor-memory 2g \
     --driver-cores 6 \
     evaluateRecom.py > outputRecom.txt 1>&1;