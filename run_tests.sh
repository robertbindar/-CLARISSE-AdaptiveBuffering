#! /bin/bash

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(pwd)/bin/"

for file in buffering/tests/*; do
  if [[ -x $file ]]
  then
    echo ">>>> Test $file started"
    ./$file $1 $2
    echo ">>>> Test $file finished"
  fi

  # let the OS free up the resources
  sleep 1
done

