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

void producer(MPI_Comm intercomm_server, MPI_Comm intracomm)
{
  int32_t rank, nprod;

  MPI_Comm_rank(intracomm, &rank);
  MPI_Comm_size(intracomm, &nprod);

  struct stat finfo;
  uint64_t bufsize = MAX_DATA;
  uint64_t file_size;

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

  double start_time = MPI_Wtime();

  uint32_t i = 0;
  cls_buf_handle_t handle;
  handle.global_descr = 0;

  while (i < chunk) {
    handle.offset = (begin + i) * bufsize;

    cls_op_put_t put;
    put.nr_participants = 1;
    put.handle = handle;
    put.offset = 0;
    if (rank == nprod - 1 && file_size % bufsize && i == chunk - 1) {
      put.count = bufsize - (nrbufs * bufsize - file_size);
    } else {
      put.count = bufsize;
    }
    memcpy(put.data, file_addr + (begin + i) * bufsize, put.count);

    cls_put_result_t result;
    MPI_Send(&put, sizeof(cls_op_put_t), MPI_CHAR, 0, 0, intercomm_server);

    MPI_Recv(&result, sizeof(cls_put_result_t), MPI_CHAR, 0, 0, intercomm_server,
             MPI_STATUS_IGNORE);

    ++i;
  }

  double end_time = MPI_Wtime();
  fprintf(stderr, "=================Producer %d time: %lf============\n", rank, end_time - start_time);

  munmap(file_addr, file_size);
  close(fd);

}

void consumer(MPI_Comm intercomm_server, MPI_Comm intracomm)
{
  int32_t rank, ncons;

  MPI_Comm_rank(intracomm, &rank);
  MPI_Comm_size(intracomm, &ncons);

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

  double start_time = MPI_Wtime();

  uint32_t i = 0;
  cls_buf_handle_t handle;
  handle.global_descr = 0;

  while (i < nrbufs) {
    handle.offset = i * bufsize;

    cls_op_get_t get;
    get.nr_participants = ncons;
    get.offset = 0;
    get.handle = handle;
    if (file_size % bufsize && i == nrbufs - 1) {
      get.count = bufsize - (nrbufs * bufsize - file_size);
    } else {
      get.count = bufsize;
    }

    cls_get_result_t result;
    MPI_Send(&get, sizeof(cls_op_get_t), MPI_CHAR, 0, 0, intercomm_server);

    MPI_Recv(&result, sizeof(cls_get_result_t), MPI_CHAR, 0, 0, intercomm_server,
             MPI_STATUS_IGNORE);

    lseek(fd, i * bufsize, SEEK_SET);
    write(fd, result.data, get.count);
    ++i;
  }

  double end_time = MPI_Wtime();

  fprintf(stderr, "=================Consumer %d time: %lf============\n", rank, end_time - start_time);

  close(fd);

  MPI_Barrier(intracomm);

  int passed = 0;
  if (rank == 0) {
    int32_t input = open("input", O_RDONLY);
    void *input_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, input, 0);

    for (i = 0; i < ncons && passed >= 0; ++i) {
      char file[100];
      sprintf(file, "%s%d", "output", i);

      int32_t output = open(file, O_RDONLY);
      void *output_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, output, 0);

      if (memcmp(input_addr, output_addr, file_size)) {
        passed = -1;
        fprintf(stderr, "--Test %s failed: %s does not match %s\n", __FILE__, file, "input");
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

    /*MPI_Abort(MPI_COMM_WORLD, passed);*/
  }
}

