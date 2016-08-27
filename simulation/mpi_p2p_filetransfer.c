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

uint64_t MAX_DATA = 1048576;

void producer()
{
  int32_t rank, nprod;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprod);

  char *bs = getenv("BUFFERING_BUFFER_SIZE");
  if (bs) {
    sscanf(bs, "%ld", &MAX_DATA);
  }

  nprod = nprod / 2;

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

  uint32_t i = 0;

  int dest_cons = rank + nprod;

  double prod_time = 0;

  char *data = malloc(MAX_DATA);
  while (i < chunk) {
    uint32_t count;
    if (rank == nprod - 1 && file_size % bufsize && i == chunk - 1) {
      count = bufsize - (nrbufs * bufsize - file_size);
    } else {
      count = bufsize;
    }

    memcpy(data, file_addr + (begin + i) * bufsize, count);
    double start_time = MPI_Wtime();
    MPI_Send(data, count, MPI_CHAR, dest_cons, 0, MPI_COMM_WORLD);
    double end_time = MPI_Wtime();

    prod_time += (end_time - start_time);
    ++i;
  }

  munmap(file_addr, file_size);
  close(fd);
  free(data);

  fprintf(stderr, "Producer rank %d, time: %lf\n", rank + nprod, prod_time);
}

void consumer()
{
  int32_t rank, ncons;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ncons);

  char *bs = getenv("BUFFERING_BUFFER_SIZE");
  if (bs) {
    sscanf(bs, "%ld", &MAX_DATA);
  }

  ncons = ncons / 2;
  rank = rank - ncons;

  int32_t source_prod = rank;

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
  double cons_time = 0;
  while (i < chunk) {
    uint32_t count;
    if (rank == ncons - 1 && file_size % bufsize && i == chunk - 1) {
      count = bufsize - (nrbufs * bufsize - file_size);
    } else {
      count = bufsize;
    }

    double start_time = MPI_Wtime();
    MPI_Recv(data, count, MPI_CHAR, source_prod, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    double end_time = MPI_Wtime();

    cons_time += (end_time - start_time);
    lseek(fd, (begin + i) * bufsize, SEEK_SET);
    write(fd, data, count);
    ++i;
  }

  close(fd);
  free(data);
  fprintf(stderr, "Consumer rank %d, time: %lf\n", rank + ncons, cons_time);
}

int main(int argc, char **argv)
{
  int32_t rank, nprocs, membership_key;
  MPI_Comm comm;

  int32_t provided;

  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  if (provided != MPI_THREAD_MULTIPLE) {
    return -1;
  }

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  uint32_t nr_producers = 1;
  uint32_t nr_consumers = 1;

  char *np = getenv("BUFFERING_NUMBER_OF_PRODUCERS");
  char *nc = getenv("BUFFERING_NUMBER_OF_CONSUMERS");

  if (np && nc) {
    sscanf(np, "%d", &nr_producers);
    sscanf(nc, "%d", &nr_consumers);
  } else {
    printf("Warning: Default decoupling values will be used\n");
  }

  membership_key = ((rank / nr_producers) != 0);

  if (membership_key == 0) {
    producer();
  } else {
    consumer();
  }

  MPI_Finalize();

  return 0;
}
