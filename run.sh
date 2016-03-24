#! /bin/bash

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(pwd)/bin/"

BUFFERING_NUMBER_OF_PRODUCERS=4 \
BUFFERING_NUMBER_OF_CONSUMERS=4 \
BUFFERING_NUMBER_OF_SERVERS=1 \
mpiexec -n 9 ./bin/producer_consumer_decoupling

