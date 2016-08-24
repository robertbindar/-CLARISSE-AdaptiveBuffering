#! /bin/bash

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(pwd)/../bin/"

block_size=1048576
blocks_count=100
input="input"

dd if=/dev/urandom of=$input bs=$block_size count=$blocks_count &> /dev/null

nprod=2
ncons=2
nserv=1
nlist=1

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(pwd)/bin/"
export BUFFERING_NUMBER_OF_PRODUCERS=$nprod
export BUFFERING_NUMBER_OF_CONSUMERS=$ncons
export BUFFERING_NUMBER_OF_SERVERS=$nserv
export BUFFERING_NR_SERVER_LISTENERS=$nlist
export BUFFERING_MAX_POOL_SIZE=120

for file in bin/cpp_test*; do
  echo ">>>> Test $file started"
  ./$file $input $nprod $ncons
  echo ">>>> Test $file finished"
done

for file in bin/mpi_*; do
  echo ">>>> Test $file started"
  mpiexec -n $(($nprod + $ncons + $nserv)) ./$file $input
  echo ">>>> Test $file finished"
done
ulimit -s unlimited
mpiexec -n $(($nprod + $ncons + $nserv)) ./bin/mpi_decoupled_filetransfer.bin $input

diff input output

rm $input

