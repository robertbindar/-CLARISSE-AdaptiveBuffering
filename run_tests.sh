#! /bin/bash

./build.sh clean

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(pwd)/bin/"

make -C ./tests/
for file in ./tests/*; do
  if [[ -x $file ]]
  then
    echo ">>>> Test $file started"
    BUFFERING_NUMBER_OF_PRODUCERS=$1 \
    BUFFERING_NUMBER_OF_CONSUMERS=$2 \
    BUFFERING_NUMBER_OF_SERVERS=$3 \
    mpiexec -n $(($1 + $2 + $3))  $file
    echo ">>>> Test $file finished"
  fi
done

