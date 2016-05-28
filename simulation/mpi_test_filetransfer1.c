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

// The input file is devided in blocks of fixed size. Each producer receives a
// number of blocks and for each block will issue a cls_put call.

double prod_time = 0;
double cons_time = 0;

void producer(MPI_Comm intercomm_server, MPI_Comm intracomm)
{
  int32_t rank, nprod, nserv;

  MPI_Comm_rank(intracomm, &rank);
  MPI_Comm_size(intracomm, &nprod);
  MPI_Comm_remote_size(intercomm_server, &nserv);

  struct stat finfo;
  uint64_t bufsize = MAX_DATA;
  uint64_t file_size;

  // Distribute the producers evenly between servers
  int32_t dest_server = rank % nserv;

  int32_t fd = open("input", O_RDONLY);
  fstat(fd, &finfo);
  file_size = finfo.st_size;

  void *file_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);

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
    put.nr_participants = 1;
    put.handle = handle;
    put.offset = 0;
    put.quit = 0;
    if (rank == nprod - 1 && file_size % bufsize && i == chunk - 1) {
      put.count = bufsize - (nrbufs * bufsize - file_size);
    } else {
      put.count = bufsize;
    }
    memcpy(put.data, file_addr + (begin + i) * bufsize, put.count);

    cls_put_result_t result;
    MPI_Send(&put, sizeof(cls_op_put_t), MPI_CHAR, dest_server, 4, intercomm_server);

    MPI_Recv(&result, sizeof(cls_put_result_t), MPI_CHAR, dest_server, 4, intercomm_server,
             MPI_STATUS_IGNORE);

    prod_time += result.time;

    ++i;
  }

  munmap(file_addr, file_size);
  close(fd);

  cls_put_result_t result;
  cls_op_put_t quit;
  quit.quit = 1;
  MPI_Send(&quit, sizeof(cls_op_put_t), MPI_CHAR, dest_server, 4, intercomm_server);

  MPI_Recv(&result, sizeof(cls_put_result_t), MPI_CHAR, dest_server, 4, intercomm_server,
      MPI_STATUS_IGNORE);

  fprintf(stderr, "Producer rank %d time: %lf\n", rank, prod_time);
}

void consumer(MPI_Comm intercomm_server, MPI_Comm intracomm)
{
  int32_t rank, ncons, nserv;

  MPI_Comm_rank(intracomm, &rank);
  MPI_Comm_size(intracomm, &ncons);
  MPI_Comm_remote_size(intercomm_server, &nserv);

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
  sprintf(filename, "%s%d", "output", rank);

  int32_t fd = open(filename, O_WRONLY | O_CREAT, S_IRWXU);
  ftruncate(fd, file_size);

  uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);
  uint32_t chunk = nrbufs / ncons;
  uint32_t begin = rank * chunk;
  if (rank == ncons - 1) {
    chunk = nrbufs - chunk * (ncons - 1);
  }

  uint32_t i = 0;
  cls_buf_handle_t handle;
  handle.global_descr = 0;

  while (i < chunk) {
    handle.offset = (begin + i) * bufsize;

    cls_op_get_t get;
    get.nr_participants = 1;
    get.offset = 0;
    get.handle = handle;
    get.quit = 0;
    if (rank == ncons - 1 && file_size % bufsize && i == chunk - 1) {
      get.count = bufsize - (nrbufs * bufsize - file_size);
    } else {
      get.count = bufsize;
    }

    cls_get_result_t result;
    MPI_Send(&get, sizeof(cls_op_get_t), MPI_CHAR, dest_server, 5, intercomm_server);

    MPI_Recv(&result, sizeof(cls_get_result_t), MPI_CHAR, dest_server, 5, intercomm_server,
             MPI_STATUS_IGNORE);
    cons_time += result.time;

    lseek(fd, (begin + i) * bufsize, SEEK_SET);
    write(fd, result.data, get.count);
    ++i;
  }

  close(fd);

  MPI_Barrier(intracomm);

  cls_get_result_t result;
  cls_op_get_t quit;
  quit.quit = 1;
  MPI_Send(&quit, sizeof(cls_op_get_t), MPI_CHAR, dest_server, 5, intercomm_server);
  MPI_Recv(&result, sizeof(cls_get_result_t), MPI_CHAR, dest_server, 5, intercomm_server,
           MPI_STATUS_IGNORE);
  fprintf(stderr, "Consumer rank %d time: %lf\n", rank, cons_time);

  MPI_Barrier(intracomm);

  if (rank == 0) {
    int32_t passed = 0;
    int32_t input = open("input", O_RDONLY);
    void *input_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, input, 0);

    for (i = 0; i < ncons && passed >= 0; ++i) {
      char file[100];
      sprintf(file, "%s%d", "output", i);

      int32_t output = open(file, O_RDONLY);
      void *output_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, output, 0);

      uint32_t ch = nrbufs / ncons;
      uint32_t size = ch * bufsize;
      if (i == ncons - 1) {
        size = file_size - ch * (ncons - 1) * bufsize;
      }

      if (memcmp((char *)input_addr + ch * i * bufsize, (char *) output_addr + ch * i * bufsize, size)) {
        passed = -1;
        fprintf(stderr, "--Test %s failed: %s does not match input\n", __FILE__, file);
      }

      close(output);
      munmap(output_addr, file_size);
      unlink(file);
    }

    if (passed >= 0) {
      fprintf(stderr, "++Test %s passed\n", __FILE__);
    }

    munmap(input_addr, file_size);
    close(input);
  }
}

#include "benchmarking.h"

#define DEFAULT_NR_LISTENERS 1
#define DEFAULT_MAX_POOL_SIZE 1024

static cls_buffering_t bufservice;
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

  init_benchmarking(rank, ncons, nprod, nserv);

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
  destroy_benchmarking();
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
  error_code err = BUFFERING_SUCCESS;
  double start_time, end_time;

  while (quit != nprod_sending) {
    MPI_Recv(&op_put, sizeof(cls_op_put_t), MPI_CHAR, MPI_ANY_SOURCE, 4, lst->communicator,
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

    MPI_Send(&result, sizeof(cls_put_result_t), MPI_CHAR, status.MPI_SOURCE, 4,
             lst->communicator);

    pthread_mutex_lock(&g_lock);
    print_counters(&bufservice);
    pthread_mutex_unlock(&g_lock);
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
  error_code err = BUFFERING_SUCCESS;
  cls_op_get_t op_get;
  double start_time, end_time;

  while (quit != ncons_sending) {
    MPI_Recv(&op_get, sizeof(cls_op_get_t), MPI_CHAR, MPI_ANY_SOURCE, 5, lst->communicator,
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

    MPI_Send(&result, sizeof(cls_get_result_t), MPI_CHAR, status.MPI_SOURCE, 5,
             lst->communicator);

    pthread_mutex_lock(&g_lock);
    print_counters(&bufservice);
    pthread_mutex_unlock(&g_lock);
  }

  pthread_exit(NULL);
}

