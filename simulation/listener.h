#pragma once

#include "pthread.h"
#include "mpi.h"
#include "stdint.h"

typedef struct listener
{
  void *(*handler)(void*);
  pthread_t tid;
  MPI_Comm communicator;
} listener_t;

int dispatch_listener(listener_t *listener);

void listener_init(listener_t *l, void *(*handler)(void*), MPI_Comm comm);

void wait_listener(listener_t *l);

