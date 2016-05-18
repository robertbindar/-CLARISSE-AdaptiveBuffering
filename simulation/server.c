#include "server.h"
#include "listener.h"
#include "utils.h"
#include <stdlib.h>
#include <stdio.h>
#include "cls_buffering.h"

#define DEFAULT_NR_LISTENERS 1
#define DEFAULT_MAX_POOL_SIZE 1024

static cls_buffering_t bufservice;

void server(MPI_Comm intercomm_producer, MPI_Comm intercomm_consumer)
{
  uint32_t i = 0;
  uint32_t nr_listeners = DEFAULT_NR_LISTENERS;
  uint32_t max_pool_size = DEFAULT_MAX_POOL_SIZE;
  char *nl = getenv("BUFFERING_NR_SERVER_LISTENERS");
  if (nl) {
    sscanf(nl, "%d", &nr_listeners);
  } else {
    printf("Warning: Default number of listeners per server will be used\n");
  }

  char *max_pool = getenv("BUFFERING_MAX_POOL_SIZE");
  if (max_pool) {
    sscanf(max_pool, "%d", &max_pool_size);
  } else {
    printf("Warning: Default max pool size will be used\n");
  }


  cls_init_buffering(&bufservice, MAX_DATA, max_pool_size);

  listener_t *listeners = calloc(2 * nr_listeners, sizeof(listener_t));
  for (i = 0; i < nr_listeners; ++i) {
    listener_init(&listeners[i], producer_handler, intercomm_producer);
    dispatch_listener(&listeners[i]);

    listener_init(&listeners[2 * i + 1], consumer_handler, intercomm_consumer);
    dispatch_listener(&listeners[2 * i + 1]);
  }

  for (i = 0; i < 2 * nr_listeners; ++i) {
    wait_listener(&listeners[i]);
  }

  free(listeners);

  cls_destroy_buffering(&bufservice);
}

void *producer_handler(void *arg)
{
  MPI_Status status;
  int32_t nprod, nserv, rank;

  listener_t *lst = (listener_t*) arg;

  MPI_Comm_rank(lst->communicator, &rank);
  MPI_Comm_size(lst->communicator, &nserv);
  MPI_Comm_remote_size(lst->communicator, &nprod);

  // nprod_sending is the number of producers that will send tasks to this server.
  // The producers are uniformly distributed over the servers, i.e. some of the last
  // servers(bigger rank) will have to wait for less producers.
  uint32_t nprod_sending = nprod / nserv + (nprod % nserv != 0);
  uint32_t nlast = nserv - nprod % nserv;
  if (nprod % nserv && rank + nlast >= nserv) {
    nprod_sending = nprod / nserv;
  }

  uint32_t quit = 0;
  cls_op_put_t op_put;
  error_code err;
  double start_time, end_time;

  while (quit != nprod_sending) {
    MPI_Recv(&op_put, sizeof(cls_op_put_t), MPI_CHAR, MPI_ANY_SOURCE, 0, lst->communicator,
             &status);
    if (op_put.quit) {
      ++quit;
      start_time = end_time = 0;
    } else if (op_put.nr_participants <= 1) {
      start_time = MPI_Wtime();
      err = cls_put(&bufservice, op_put.handle, op_put.offset, op_put.data, op_put.count);
      end_time = MPI_Wtime();
    } else {
      start_time = MPI_Wtime();
      err = cls_put_all(&bufservice, op_put.handle, op_put.offset, op_put.data,
                           op_put.count, op_put.nr_participants);
      end_time = MPI_Wtime();
    }

    cls_put_result_t result;

    result.time = end_time - start_time;

    result.status = (uint32_t) err;

    MPI_Send(&result, sizeof(cls_put_result_t), MPI_CHAR, status.MPI_SOURCE, 0,
             lst->communicator);
  }

  pthread_exit(NULL);
}

void *consumer_handler(void *arg)
{
  MPI_Status status;
  int32_t ncons, nserv, rank;

  listener_t *lst = (listener_t*) arg;

  MPI_Comm_rank(lst->communicator, &rank);
  MPI_Comm_size(lst->communicator, &nserv);
  MPI_Comm_remote_size(lst->communicator, &ncons);

  // ncons_sending is the number of consumers that will send tasks to this server.
  // The consumers are uniformly distributed over the servers, i.e. some of the last
  // servers(bigger rank) will have to wait for less consumers.
  uint32_t ncons_sending = ncons / nserv + (ncons % nserv != 0);
  uint32_t nlast = nserv - ncons % nserv;
  if (ncons % nserv && rank + nlast >= nserv) {
    ncons_sending = ncons / nserv;
  }

  uint32_t quit = 0;
  error_code err;
  cls_op_get_t op_get;
  double start_time, end_time;

  while (quit != ncons_sending) {
    MPI_Recv(&op_get, sizeof(cls_op_get_t), MPI_CHAR, MPI_ANY_SOURCE, 0, lst->communicator,
             &status);

    cls_get_result_t result;
    if (op_get.quit) {
      ++quit;
      start_time = end_time = 0;
    } else {
      start_time = MPI_Wtime();
      err = cls_get(&bufservice, op_get.handle, op_get.offset, result.data, op_get.count,
                    op_get.nr_participants);
      end_time = MPI_Wtime();
    }

    result.time = end_time - start_time;

    result.status = (uint32_t) err;

    MPI_Send(&result, sizeof(cls_get_result_t), MPI_CHAR, status.MPI_SOURCE, 0,
             lst->communicator);
  }

  pthread_exit(NULL);
}
