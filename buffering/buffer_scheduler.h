/* vim: set ts=8 sts=2 et sw=2: */

#pragma once

#include <stdint.h>
#include "buffer_allocator.h"
#include "errors.h"

typedef struct
{
  buffer_allocator_t allocator;

  uint64_t nr_free_buffers;
  uint64_t nr_assigned_buffers;

  uint64_t max_free_buffers;
  uint64_t min_free_buffers;

  uint64_t max_pool_size;

} buffer_scheduler_t;

//error_code sched_get_buffer(buffer_scheduler_t *scheduler, 
