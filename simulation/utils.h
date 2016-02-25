#pragma once

#include "mpi.h"
#include "stdio.h"


void handle_err(int errcode, char *str) {
  char msg[MPI_MAX_ERROR_STRING];
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int namelen, resultlen, my_rank;

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Get_processor_name(processor_name,&namelen);
  MPI_Error_string(errcode, msg, &resultlen);
  fprintf(stderr,"Process %d on %s %s: %s\nStack trace:\n", my_rank, processor_name,str, msg);
  MPI_Abort(MPI_COMM_WORLD, 1);
}

