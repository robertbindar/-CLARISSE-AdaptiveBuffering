#!/bin/bash

maxbuf=20
inputsize=$((1024 * 1024 * 1024))
nrserv=4

mkdir -p test;


dir=1
# Create directories name 2 4 8 16..2^20
# The name of the subdirectory specifies the size of the buffer used for simulation
# insert a script.pbs file personalized for each buffer size in each subdirectory
# submit the bufferflow and the tightly coupled mpi simulation
for i in $(seq 1 $maxbuf); do
  dir=$(($dir * 2))

  mkdir -p test/dir-$dir

  cp ./input test/dir-$dir/
  cp ./script.pbs test/dir-$dir/script_decoupled.pbs
  echo "export BUFFERING_BUFFER_SIZE=$dir" >> test/dir-$dir/script_decoupled.pbs
  echo "export BUFFERING_MAX_POOL_SIZE=$(($inputsize / $nrserv / $dir))" >> test/dir-$dir/script_decoupled.pbs
  echo "name=decoupled" >> test/dir-$dir/script_decoupled.pbs
  echo 'aprun -j 2 -n $(($nprod + $ncons + $nserv)) /work/pr1u1352/pr1u1352/pr1e1903/CLARISSE-AdaptiveBuffering/bin/mpi_decoupled_filetransfer.bin &> ""$name"_"$nprod"_"$ncons"_"$nserv"' >> test/dir-$dir/script_decoupled.pbs

  cp ./script.pbs test/dir-$dir/script_p2p.pbs
  echo "export BUFFERING_BUFFER_SIZE=$dir" >> test/dir-$dir/script_p2p.pbs
  echo "export BUFFERING_MAX_POOL_SIZE=$(($inputsize / $nrserv / $dir))" >> test/dir-$dir/script_p2p.pbs
  echo "name=decoupled" >> test/dir-$dir/script_p2p.pbs
  echo 'aprun -j 2 -n $(($nprod + $ncons + $nserv)) /work/pr1u1352/pr1u1352/pr1e1903/CLARISSE-AdaptiveBuffering/bin/mpi_p2p_filetransfer.bin &> ""$name"_"$nprod"_"$ncons"_"$nserv"' >> test/dir-$dir/script_p2p.pbs

  cd test/dir-$dir
  # block=true is supposed to block qsub until the job is done.
  # The queues have limits for the number of jobs you can submit, it's less efficient to block but this way you won't have to
  # keep an eye on the script and see if all the jobs were accepted in the queue's waiting list
  qsub -W block=true ./script_decoupled.pbs
  qsub -W block=true ./script_p2p.pbs

  # process results
  touch decoupled_results
  cat decoupled_64_64_4 | grep Consumer | cut -d' ' -f5 | tr '\n' ' ' | awk '{s+=$1}END{print NR,s," ",s/NR}' RS=" " >> decoupled_results
  echo -n " " >> decoupled_results
  cat decoupled_64_64_4 | grep Producer | cut -d' ' -f5 | tr '\n' ' ' | awk '{s+=$1}END{print NR,s," ",s/NR}' RS=" " >> decoupled_results

  touch p2p_results
  cat p2p_64_64_4 | grep Consumer | cut -d' ' -f5 | tr '\n' ' ' | awk '{s+=$1}END{print NR,s," ",s/NR}' RS=" " >> p2p_results
  echo -n " " >> p2p_results
  cat decoupled_64_64_4 | grep Producer | cut -d' ' -f5 | tr '\n' ' ' | awk '{s+=$1}END{print NR,s," ",s/NR}' RS=" " >> p2p_results

  cd ../../
done

#gather all the results together
#the results file will contain the average participant execution time:
#
#   bufsize decoupled_consumer decoupled_producer p2p_consumer p2p_producer
#     2           1.01              0.03              0.112       0.5
#     4           1.01              0.03              0.112       0.5
#     8           1.01              0.03              0.112       0.5
#    16           1.01              0.03              0.112       0.5
rez=final_results
touch $rez

dir=1
for i in $(seq 1 $maxbuf); do
  dir=$(($dir * 2))
  cons1=$(cat test/dir-$dir/decoupled_results | cut -d' ' -f1)
  prod1=$(cat test/dir-$dir/decoupled_results | cut -d' ' -f2)
  cons2=$(cat test/dir-$dir/p2p_results | cut -d' ' -f1)
  prod2=$(cat test/dir-$dir/p2p_results | cut -d' ' -f2)
  echo "$dir $con1 $prod1 $cons2 $prod2" >> $rez
done
