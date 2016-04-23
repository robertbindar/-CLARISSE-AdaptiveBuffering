#include "buffer_swapper.h"
#include <sys/types.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

static void copy_buf_handle(cls_buf_handle_t *dest, cls_buf_handle_t *src)
{
  memset(dest, 0, sizeof(cls_buf_handle_t));
  dest->offset = src->offset;
  dest->global_descr = src->global_descr;
}

void swapper_init(buffer_swapper_t *sw, uint64_t bufsize)
{
  sprintf(sw->dirname, "%s", ".swaparea/");
  sw->bufsize = bufsize;
  sw->entries = NULL;
  pthread_mutex_init(&sw->lock, NULL);

  mkdir(sw->dirname, S_IRWXU);
}

void swapper_swapin(buffer_swapper_t *sw, cls_buf_t *buf)
{
  char filename[MAX_FILENAME_SIZE];
  sprintf(filename, "%s%" PRIu32, sw->dirname, buf->handle.global_descr);
  int32_t fd = open(filename, O_RDONLY);

  pthread_mutex_lock(&sw->lock);

  swap_entry_t *found;
  HASH_FIND(hh, sw->entries, &buf->handle, sizeof(cls_buf_handle_t), found);

  lseek(fd, found->file_offset, SEEK_SET);
  read(fd, buf->data, sw->bufsize);

  HASH_DEL(sw->entries, found);

  pthread_mutex_unlock(&sw->lock);

  buf->is_swapped = 0;

  close(fd);
  free(found);
}

void swapper_swapout(buffer_swapper_t *sw, cls_buf_t *buf)
{
  pthread_rwlock_wrlock(&buf->rwlock_swap);

  uint8_t consumers_finished = 0;
  pthread_mutex_lock(&buf->lock_read);
  consumers_finished = buf->consumers_finished;
  pthread_mutex_unlock(&buf->lock_read);

  // If the all the consumers finished reading the buffers, it's reasonable
  // to assume they will free up the buffer faster.
  if (consumers_finished) {
    pthread_rwlock_unlock(&buf->rwlock_swap);
    return;
  }

  swap_entry_t *entry = calloc(1, sizeof(swap_entry_t));
  char filename[MAX_FILENAME_SIZE];
  sprintf(filename, "%s%" PRIu32, sw->dirname, buf->handle.global_descr);
  int32_t fd = open(filename, O_RDWR | O_CREAT | O_APPEND, S_IRWXU);

  pthread_mutex_lock(&sw->lock);

  entry->file_offset = lseek(fd, 0, SEEK_END);
  write(fd, buf->data, sw->bufsize);
  close(fd);

  copy_buf_handle(&entry->handle, &buf->handle);
  HASH_ADD(hh, sw->entries, handle, sizeof(cls_buf_handle_t), entry);

  pthread_mutex_unlock(&sw->lock);

  buf->is_swapped = 1;
  pthread_rwlock_unlock(&buf->rwlock_swap);
}

void swapper_destroy(buffer_swapper_t *sw)
{
  swap_entry_t *current, *tmp;
  HASH_ITER(hh, sw->entries, current, tmp) {
    HASH_DEL(sw->entries, current);
  }

  pthread_mutex_destroy(&sw->lock);
  // TODO: write rm -rf function
}

