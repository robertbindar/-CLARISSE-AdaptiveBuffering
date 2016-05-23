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

  build_types();

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
    MPI_Send(&put, 1, mpi_op_put_t, dest_server, 0, intercomm_server);

    MPI_Recv(&result, 1, mpi_put_result_t, dest_server, 0, intercomm_server,
             MPI_STATUS_IGNORE);

    prod_time += result.time;

    ++i;
  }

  munmap(file_addr, file_size);
  close(fd);

  cls_put_result_t result;
  cls_op_put_t quit;
  quit.quit = 1;
  MPI_Send(&quit, 1, mpi_op_put_t, dest_server, 0, intercomm_server);

  MPI_Recv(&result, 1, mpi_put_result_t, dest_server, 0, intercomm_server,
      MPI_STATUS_IGNORE);

  fprintf(stderr, "Producer rank %d time: %lf\n", rank, prod_time);

  destroy_types();
}

void consumer(MPI_Comm intercomm_server, MPI_Comm intracomm)
{
  int32_t rank, ncons, nserv;

  MPI_Comm_rank(intracomm, &rank);
  MPI_Comm_size(intracomm, &ncons);
  MPI_Comm_remote_size(intercomm_server, &nserv);

  build_types();

  // Distribute the consumers evenly between servers
  int32_t dest_server = rank % nserv;

  struct stat finfo;
  uint64_t bufsize = MAX_DATA;
  uint32_t file_size;

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
    MPI_Send(&get, 1, mpi_op_get_t, dest_server, 0, intercomm_server);

    MPI_Recv(&result, 1, mpi_get_result_t, dest_server, 0, intercomm_server,
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
  MPI_Send(&quit, 1, mpi_op_get_t, dest_server, 0, intercomm_server);
  MPI_Recv(&result, 1, mpi_get_result_t, dest_server, 0, intercomm_server,
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

  destroy_types();
}

