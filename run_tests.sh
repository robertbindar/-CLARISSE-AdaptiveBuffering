#! /bin/bash

./build.sh clean

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(pwd)/bin/"

make -C buffering/tests
for file in buffering/tests/*; do
  if [[ -x $file ]]
  then
    echo ">>>> Test $file started"
    ./$file $1 $2
    echo ">>>> Test $file finished"
  fi
done

