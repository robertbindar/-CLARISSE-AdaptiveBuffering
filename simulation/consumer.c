#include <stdio.h>
#include "utils.h"
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "consumer.h"


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
  fprintf(stderr, "finished_consumer\n");

  close(fd);
}

