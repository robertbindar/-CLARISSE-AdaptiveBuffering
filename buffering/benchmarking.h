#include <stdio.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <unistd.h>
#include "cls_buffering.h"

#define FILENAME "benchmarking_ondemand_"

static FILE *out = NULL;

static void init_benchmarking(int32_t server_rank, uint32_t ncons, uint32_t nprod)
{
  char file[256];
  sprintf(file, "%s%" PRIu32 "_%" PRIu32, FILENAME, nprod, ncons);
  out = fopen(file, "w");
}

static void print_counters(cls_buffering_t *bufservice)
{
  uint64_t bufs_count = 0;

  pthread_mutex_lock(&(bufservice->buf_sched.lock));
  bufs_count = bufservice->buf_sched.nr_free_buffers +
               bufservice->buf_sched.nr_assigned_buffers;
  pthread_mutex_unlock(&(bufservice->buf_sched.lock));

  fprintf(out, "%" PRIu64 "\n", bufs_count);
}

static void destroy_benchmarking()
{
  fclose(out);
}

