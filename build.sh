#! /bin/bash

if [[ $1 = "clean" ]]; then
  make clean -C buffering
  #make clean -C simulation
  make clean -C buffering/tests/
  make clean -C tests/
fi
BINARIES=bin/
INCLUDE=include/

mkdir -p $BINARIES
mkdir -p $INCLUDE

cp buffering/*.h $INCLUDE
cp simulation/*.h $INCLUDE

make -C buffering/
mv buffering/libbuffering.so $BINARIES

#make -C simulation/
#mv simulation/producer_consumer_decoupling $BINARIES

make -C buffering/tests/

make -C tests/

