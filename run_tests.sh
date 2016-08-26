#! /bin/bash

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(pwd)/../bin/"

block_size=1048576
blocks_count=10
input="input"
bufsize=$((1024 * 1024))
nrbufs=$(($block_size * $blocks_count / $bufsize))

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
export BUFFERING_BUFFER_SIZE=$bufsize
export BUFFERING_MAX_POOL_SIZE=$nrbufs

for file in bin/cpp_test*; do
  echo ">>>> Test $file started"
  ./$file $input $nprod $ncons
  echo ">>>> Test $file finished"
done

for file in bin/mpi_*; do
  if [ "$file" == "bin/mpi_p2p_filetransfer.bin" ]; then
    export BUFFERING_NUMBER_OF_SERVERS=0
    mpiexec -n $(($nprod + $ncons)) ./$file $input
    continue
  fi
  echo ">>>> Test $file started"
  ulimit -s unlimited
  mpiexec -n $(($nprod + $ncons + $nserv)) ./$file $input
  echo ">>>> Test $file finished"
done

diff input output

rm $input

