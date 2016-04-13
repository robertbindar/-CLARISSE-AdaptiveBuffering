#include "producer.h"
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "utils.h"
#include "cls_buffering.h"

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
  if (rank == nprod - 1) {
    chunk = nrbufs - chunk * (nprod - 1);
  }
  uint32_t begin = rank * chunk;

  uint32_t i = 0;
  cls_buf_handle_t handle;
  handle.global_descr = 0;

  while (i < chunk) {
    sleep(1);
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

  munmap(file_addr, file_size);
  close(fd);

}

