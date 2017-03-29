#! /bin/bash

BINARIES=bin
SIMULATION=simulation
INCLUDE=include/
BUFFERING=buffering

if [ $# -ge 2 ]; then
  echo "Error: too many arguments"
  exit -1
else
  if [[ $1 = "clean" ]]; then
    make clean -C $BUFFERING
#    make clean -C $BUFFERING/tests
#    make clean -C $SIMULATION
    rm -rf $BINARIES
    exit 0
  elif [ $# -eq 1 ]
  then
    echo "Error: the argument passed was invalid"
    exit -1
  fi
fi

mkdir -p $BINARIES
mkdir -p $INCLUDE

cp $BUFFERING/*.h $INCLUDE
#cp $SIMULATION/*.h $INCLUDE

make -C $BUFFERING
#mv $BUFFERING/libbuffering.so $BINARIES/

make -C $SIMULATION
mv $SIMULATION/*.bin $BINARIES/

#make -C $BUFFERING/tests/
#mv $BUFFERING/tests/*.bin $BINARIES/

