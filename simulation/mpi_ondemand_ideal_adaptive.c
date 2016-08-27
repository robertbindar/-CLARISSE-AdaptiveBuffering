#include "producer.h"
#include "consumer.h"
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "utils.h"
#include "server.h"
#include "listener.h"
#include "cls_buffering.h"

uint64_t MAX_DATA = 1048576;
#define BURST_WAIT 200000
#define BURST_COUNT 30

#define _BENCHMARKING

double prod_time = 0;
double cons_time = 0;

void *disk_handler(void *arg);

void producer(MPI_Comm intercomm_server, MPI_Comm intracomm)
{
  int32_t rank, nprod, nserv;

  MPI_Comm_rank(intracomm, &rank);
  MPI_Comm_size(intracomm, &nprod);
  MPI_Comm_remote_size(intercomm_server, &nserv);

  char *bs = getenv("BUFFERING_BUFFER_SIZE");
  if (bs) {
    sscanf(bs, "%ld", &MAX_DATA);
  }

  struct stat finfo;
  uint64_t bufsize = MAX_DATA;
  uint64_t file_size;

  // Distribute the producers evenly between servers
  int32_t dest_server = rank % nserv;

  int32_t fd = open("input", O_RDONLY);
  fstat(fd, &finfo);
  file_size = finfo.st_size;
  close(fd);

  uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);
  uint32_t chunk = nrbufs / nprod;
  uint32_t begin = rank * chunk;
  if (rank == nprod - 1) {
    chunk = nrbufs - chunk * (nprod - 1);
  }

  uint32_t i = 0;
  cls_buf_handle_t handle;
  handle.global_descr = 0;

  while (i < chunk) {
    handle.offset = (begin + i) * bufsize;

    cls_op_put_t put;
    put.handle = handle;
    put.quit = 0;
    if (rank == nprod - 1 && file_size % bufsize && i == chunk - 1) {
      put.count = bufsize - (nrbufs * bufsize - file_size);
    } else {
      put.count = bufsize;
    }

    double start_time = MPI_Wtime();
    MPI_Send(&put, sizeof(cls_op_put_t), MPI_CHAR, dest_server, 4, intercomm_server);
    double end_time = MPI_Wtime();

    prod_time += (end_time - start_time);
    ++i;
  }

  cls_op_put_t quit;
  quit.quit = 1;
  MPI_Send(&quit, sizeof(cls_op_put_t), MPI_CHAR, dest_server, 4, intercomm_server);
}

void consumer(MPI_Comm intercomm_server, MPI_Comm intracomm)
{
  int32_t rank, ncons, nserv;

  MPI_Comm_rank(intracomm, &rank);
  MPI_Comm_size(intracomm, &ncons);
  MPI_Comm_remote_size(intercomm_server, &nserv);

  char *bs = getenv("BUFFERING_BUFFER_SIZE");
  if (bs) {
    sscanf(bs, "%ld", &MAX_DATA);
  }

  // Distribute the consumers evenly between servers
  int32_t dest_server = rank % nserv;

  struct stat finfo;
  uint64_t bufsize = MAX_DATA;
  uint64_t file_size;

  int32_t input = open("input", O_RDONLY);
  fstat(input, &finfo);
  file_size = finfo.st_size;
  close(input);

  char filename[100];
  sprintf(filename, "%s", "output");

  int32_t fd = open(filename, O_WRONLY | O_CREAT, S_IRWXU);
  ftruncate(fd, file_size);

  uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);
  uint32_t chunk = nrbufs / ncons;
  uint32_t begin = rank * chunk;
  if (rank == ncons - 1) {
    chunk = nrbufs - chunk * (ncons - 1);
  }

  uint32_t i = 0;
  char *data = malloc(MAX_DATA);

  cls_op_get_t op_get;
  op_get.handle.global_descr = 0;

  while (i < chunk) {
    uint64_t count;

    if (rank == ncons - 1 && file_size % bufsize && i == chunk - 1) {
      count = bufsize - (nrbufs * bufsize - file_size);
    } else {
      count = bufsize;
    }

    op_get.handle.offset = (begin + i) * bufsize;
    op_get.count = count;
    op_get.quit = 0;
    MPI_Send(&op_get, sizeof(cls_op_get_t), MPI_CHAR, dest_server, 5, intercomm_server);

    double start_time = MPI_Wtime();
    MPI_Recv(data, count, MPI_CHAR, dest_server, 5, intercomm_server,
             MPI_STATUS_IGNORE);
    double end_time = MPI_Wtime();

    cons_time += (end_time - start_time);
    lseek(fd, (begin + i) * bufsize, SEEK_SET);
    write(fd, data, count);
    ++i;
  }

  op_get.quit = 1;
  MPI_Send(&op_get, sizeof(cls_op_get_t), MPI_CHAR, dest_server, 5, intercomm_server);

  free(data);
  close(fd);
}

#ifdef _BENCHMARKING
#include "benchmarking.h"
#endif

#define DEFAULT_NR_LISTENERS 1
#define DEFAULT_MAX_POOL_SIZE 1024

static cls_buffering_t bufservice;
static cls_buffering_t work_queue;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

MPI_Comm server_comm;

void server(MPI_Comm intercomm_producer, MPI_Comm intercomm_consumer, MPI_Comm intracomm)
{
  int32_t rank, nprod, ncons, nserv;
  uint32_t i = 0;
  uint32_t nr_listeners = DEFAULT_NR_LISTENERS;
  uint32_t max_pool_size = DEFAULT_MAX_POOL_SIZE;

  MPI_Comm_rank(intracomm, &rank);
  MPI_Comm_size(intracomm, &nserv);
  MPI_Comm_remote_size(intercomm_producer, &nprod);
  MPI_Comm_remote_size(intercomm_consumer, &ncons);

  server_comm = intracomm;

#ifdef _BENCHMARKING
    init_benchmarking(rank, ncons, nprod, nserv);
#endif

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

  char *bs = getenv("BUFFERING_BUFFER_SIZE");
  if (bs) {
    sscanf(bs, "%ld", &MAX_DATA);
  }

  cls_init_buffering(&bufservice, MAX_DATA, max_pool_size);

  cls_init_buffering(&work_queue, sizeof(cls_task_t), max_pool_size);

  listener_t *listeners = calloc(2 * nr_listeners + 1, sizeof(listener_t));
  for (i = 0; i < nr_listeners; ++i) {
    listener_init(&listeners[i], producer_handler, intercomm_producer);
    dispatch_listener(&listeners[i]);

    listener_init(&listeners[2 * i + 1], consumer_handler, intercomm_consumer);
    dispatch_listener(&listeners[2 * i + 1]);
  }

  listener_init(&listeners[2 * nr_listeners], disk_handler, intercomm_consumer);
  dispatch_listener(&listeners[2 * nr_listeners]);

  for (i = 0; i < 2 * nr_listeners + 1; ++i) {
    wait_listener(&listeners[i]);
  }

  free(listeners);

#ifdef _BENCHMARKING
    destroy_benchmarking();
#endif

  cls_destroy_buffering(&bufservice);
  cls_destroy_buffering(&work_queue);
}

uint64_t nr_producers_sending;
uint64_t nr_consumers_sending;

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
  nr_producers_sending = nprod_sending;

  uint32_t nlast = nserv - nprod % nserv;
  if (nprod % nserv && rank + nlast >= nserv) {
    nprod_sending = nprod / nserv;
  }

  uint32_t quit = 0;
  cls_op_put_t op_put;
  error_code err = BUFFERING_SUCCESS;

  uint64_t task_count = 0;
  while (quit != nprod_sending) {
    MPI_Recv(&op_put, sizeof(cls_op_put_t), MPI_CHAR, MPI_ANY_SOURCE, 4, lst->communicator,
             &status);
    if (op_put.quit) {
      ++quit;
    } else {
      cls_task_t t;
      t.offset = op_put.handle.offset;
      t.count = op_put.count;
      t.quit = 0;
      t.source_rank = status.MPI_SOURCE;

      cls_buf_handle_t task_handle;
      task_handle.global_descr = task_count;
      task_handle.offset = 0;
      ++task_count;

      err = cls_put(&work_queue, task_handle, 0, (void*)&t, sizeof(cls_task_t));
    }
  }

  cls_task_t t;
  t.quit = 1;

  cls_buf_handle_t task_handle;
  task_handle.global_descr = task_count;
  task_handle.offset = 0;

  err = cls_put(&work_queue, task_handle, 0, (void*)&t, sizeof(cls_task_t));

  pthread_exit(NULL);
}

void *consumer_handler(void *arg)
{
  MPI_Status status;
  int32_t ncons, nserv, rank, total_procs;

  listener_t *lst = (listener_t*) arg;

  MPI_Comm_rank(lst->communicator, &rank);
  MPI_Comm_size(lst->communicator, &nserv);
  MPI_Comm_size(MPI_COMM_WORLD, &total_procs);
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
  cls_op_get_t op_get;

  double consumers_time = 0;
  uint64_t nr_requests = 0;

  double *consumers_times = calloc(total_procs, sizeof(double));
  int *consumers_requests = calloc(total_procs, sizeof(int));

  uint64_t burst_count = 0;
  char *data = malloc(MAX_DATA);
  while (quit != ncons_sending) {
    MPI_Recv(&op_get, sizeof(cls_op_get_t), MPI_CHAR, MPI_ANY_SOURCE, 5, lst->communicator,
             &status);

    if (op_get.quit) {
      ++quit;
    } else {
      if (burst_count == BURST_COUNT) {
        burst_count = 0;
        usleep(BURST_WAIT);
      }

      double start_time = MPI_Wtime();
      cls_get(&bufservice, op_get.handle, 0, data, op_get.count);
      double end_time = MPI_Wtime();

#ifdef _BENCHMARKING
      print_counters(&bufservice);
#endif
      consumers_requests[status.MPI_SOURCE]++;
      consumers_times[status.MPI_SOURCE] += (end_time - start_time);

      MPI_Send(data, op_get.count, MPI_CHAR, status.MPI_SOURCE, 5,
               lst->communicator);
    }

    ++burst_count;
  }

  int i;
  for (i = 0; i < total_procs; ++i) {
    if (!consumers_requests[i]) {
      continue;
    }
    fprintf(stderr, "Consumer rank %d on server rank %d, time avg: %lf\n",
            i, rank, consumers_times[i] / consumers_requests[i]);
  }

  free(data);
  pthread_exit(NULL);
}

void *disk_handler(void *arg)
{
  listener_t *lst = (listener_t*) arg;

  int rank, total_procs;
  MPI_Comm_rank(lst->communicator, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &total_procs);

  cls_buf_handle_t task_handle;
  task_handle.offset = 0;
  task_handle.global_descr = 0;
  cls_task_t task;

  int32_t fd = open("input", O_RDONLY);

  cls_buf_handle_t buf_handle;
  buf_handle.global_descr = 0;

  char *data = malloc(MAX_DATA);

  uint64_t burst_count = 0;
  double producers_time = 0;
  uint64_t nr_requests = 0;

  double *producers_times = calloc(total_procs, sizeof(double));
  int *producers_requests = calloc(total_procs, sizeof(int));
  while (1) {
    cls_get(&work_queue, task_handle, 0, (void*)&task, sizeof(cls_task_t));
    if (task.quit) {
      break;
    }

    lseek(fd, task.offset, SEEK_SET);
    read(fd, data, task.count);

    buf_handle.offset = task.offset;

    if (burst_count == BURST_COUNT) {
      burst_count = 0;
      usleep(BURST_WAIT);
    }
    double start_time = MPI_Wtime();
    cls_put(&bufservice, buf_handle, 0, data, task.count);
    double end_time = MPI_Wtime();

#ifdef _BENCHMARKING
      print_counters(&bufservice);
#endif

    producers_requests[task.source_rank]++;
    producers_times[task.source_rank] += (end_time - start_time);

    task_handle.global_descr++;
    ++burst_count;
    ++nr_requests;
  }

  close(fd);

  int i;
  for (i = 0; i < total_procs; ++i) {
    if (!producers_requests[i]) {
      continue;
    }
    fprintf(stderr, "Producer rank: %d on server rank %d, time avg: %lf\n",
            i, rank, producers_times[i] / producers_requests[i]);
  }

  free(data);
  return NULL;
}

