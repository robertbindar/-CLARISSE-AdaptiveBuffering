#include "listener.h"
#include "pthread.h"

int dispatch_listener(listener_t *listener)
{
  return pthread_create(&listener->tid, NULL, listener->handler, listener);
}

void listener_init(listener_t *l, int mpi_rank, void *(*handler)(void*), MPI_Comm comm)
{
  l->rank = mpi_rank;
  l->handler = handler;
  l->communicator = comm;
}

void wait_listener(listener_t *l)
{
  pthread_join(l->tid, NULL);
}

