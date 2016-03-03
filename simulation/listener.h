#pragma once

#include "pthread.h"
#include "mpi.h"
#include "stdint.h"

typedef struct listener_data
{
  uint32_t rank;
} listener_data_t;

typedef struct listener
{
  void *(*handler)(void*);
  pthread_t tid;
  listener_data_t data;
  MPI_Comm communicator;
  uint32_t rank;
} listener_t;

int dispatch_listener(listener_t *listener);

void listener_init(listener_t *l, int mpi_rank, void *(*handler)(void*), MPI_Comm comm);

void wait_listener(listener_t *l);

