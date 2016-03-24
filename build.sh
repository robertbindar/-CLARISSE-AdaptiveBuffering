#! /bin/bash

BINARIES=bin/
INCLUDE=include/

mkdir -p $BINARIES
mkdir -p $INCLUDE

cp buffering/*.h $INCLUDE
cp simulation/*.h $INCLUDE

make -C buffering/
mv buffering/libbuffering.so $BINARIES

make -C simulation/
mv simulation/producer_consumer_decoupling $BINARIES

