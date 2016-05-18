#! /bin/bash

debug=''
if [[ $1 = "debug" ]]; then
  debug='xterm -e gdb'
fi

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(pwd)/../bin/"

nprod=2
ncons=2
nserv=2
nlist=1

BUFFERING_NUMBER_OF_PRODUCERS=$nprod \
BUFFERING_NUMBER_OF_CONSUMERS=$ncons \
BUFFERING_NUMBER_OF_SERVERS=$nserv   \
BUFFERING_NR_SERVER_LISTENERS=$nlist \
BUFFERING_MAX_POOL_SIZE=500000       \
mpiexec -n $(($nprod + $ncons + $nserv)) $debug ./test_NM1_filetransfer1

