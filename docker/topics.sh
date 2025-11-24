#!/bin/bash

/opt/kafka/bin/kafka-topics.sh \
   --bootstrap-server kafka:9093 \
   --create --topic default \
   --partitions 1 \
   --replication-factor 1
