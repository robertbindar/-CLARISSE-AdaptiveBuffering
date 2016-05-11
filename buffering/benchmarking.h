#include <stdio.h>
#include <inttypes.h>
#include <unistd.h>
#include "cls_buffering.h"

#define FILENAME "benchmarking_swapping_"

static void init_benchmarking(uint32_t ncons, uint32_t nprod)
{
  char file[256];
  sprintf(file, "%s%" PRIu32 "_%" PRIu32, FILENAME, nprod, ncons);

  unlink(file);
}

static void print_counters(cls_buffering_t *bufservice, uint32_t ncons, uint32_t nprod)
{
  uint64_t bufs_count = 0;

  char file[256];
  sprintf(file, "%s%" PRIu32 "_%" PRIu32, FILENAME, nprod, ncons);

  FILE *out = fopen(file, "a+");
  pthread_mutex_lock(&(bufservice->buf_sched.lock));
  bufs_count = bufservice->buf_sched.nr_free_buffers +
               bufservice->buf_sched.nr_assigned_buffers;
  pthread_mutex_unlock(&(bufservice->buf_sched.lock));
  fprintf(out, "%" PRIu64 "\n", bufs_count);
  fclose(out);
}

