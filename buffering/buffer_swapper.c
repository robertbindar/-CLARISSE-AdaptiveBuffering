/* vim: set ts=8 sts=2 et sw=2: */

#include "buffer_swapper.h"
#include <sys/types.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <dirent.h>

void swapper_init(buffer_swapper_t *sw, uint64_t bufsize)
{
  sprintf(sw->dirname, "%s", ".swaparea/");
  sw->bufsize = bufsize;
  sw->entries = NULL;
  sw->entries_count = 0;
  pthread_mutex_init(&sw->lock, NULL);
  dllist_init(&sw->disk_free_offsets);

  mkdir(sw->dirname, S_IRWXU);
}

void swapper_swapin(buffer_swapper_t *sw, cls_buf_t *buf)
{
  char filename[MAX_FILENAME_SIZE];
  sprintf(filename, "%s%" PRIu32, sw->dirname, buf->handle.global_descr);
  int32_t fd = open(filename, O_RDONLY);

  pthread_mutex_lock(&sw->lock);

  swap_entry_t *found;
  HASH_FIND_PTR(sw->entries, &buf, found);

  lseek(fd, found->file_offset, SEEK_SET);
  read(fd, buf->data, sw->bufsize);

  HASH_DEL(sw->entries, found);
  sw->entries_count--;

  free_off_t *f = malloc(sizeof(free_off_t));
  f->offset = found->file_offset;
  dllist_iat(&sw->disk_free_offsets, &f->link);

  free(found);
  pthread_mutex_unlock(&sw->lock);

  buf->is_swapped = 0;

  close(fd);
}

uint8_t swapper_find(buffer_swapper_t *sw, cls_buf_t *buf)
{
  pthread_mutex_lock(&sw->lock);

  swap_entry_t *found;
  HASH_FIND_PTR(sw->entries, &buf, found);

  uint8_t result = (found != NULL);

  pthread_mutex_unlock(&sw->lock);

  return result;
}

cls_buf_t *swapper_top(buffer_swapper_t *sw)
{
  pthread_mutex_lock(&sw->lock);

  swap_entry_t *found = NULL, *tmp;
  HASH_ITER(hh, sw->entries, found, tmp) {
    break;
  }

  cls_buf_t *buf = NULL;
  if (found) {
    buf = found->buf;
  }

  pthread_mutex_unlock(&sw->lock);

  return buf;
}

void swapper_swapout(buffer_swapper_t *sw, cls_buf_t *buf)
{
  // This function might be called from the swapping thread, the buffer needs
  // to be protected so that this function won't interfere with any consumer thread.
  pthread_mutex_lock(&buf->lock_read);
  swapper_swapout_lockfree(sw, buf);
  pthread_mutex_unlock(&buf->lock_read);
}

void swapper_swapout_lockfree(buffer_swapper_t *sw, cls_buf_t *buf)
{
  swap_entry_t *entry = calloc(1, sizeof(swap_entry_t));
  char filename[MAX_FILENAME_SIZE];
  sprintf(filename, "%s%" PRIu32, sw->dirname, buf->handle.global_descr);
  int32_t fd = open(filename, O_RDWR | O_CREAT | O_APPEND, S_IRWXU);

  pthread_mutex_lock(&sw->lock);

  if (!dllist_is_empty(&sw->disk_free_offsets)) {
    dllist_link *tmp = dllist_rem_head(&sw->disk_free_offsets);
    free_off_t *f = DLLIST_ELEMENT(tmp, free_off_t, link);
    entry->file_offset = lseek(fd, f->offset, SEEK_SET);
    free(f);
  } else {
    entry->file_offset = lseek(fd, 0, SEEK_END);
  }
  write(fd, buf->data, sw->bufsize);
  close(fd);

  entry->buf = buf;
  HASH_ADD_PTR(sw->entries, buf, entry);
  sw->entries_count++;

  pthread_mutex_unlock(&sw->lock);

  buf->is_swapped = 1;
}

uint64_t swapper_getcount(buffer_swapper_t *sw)
{
  pthread_mutex_lock(&sw->lock);
  uint64_t count = sw->entries_count;
  pthread_mutex_unlock(&sw->lock);

  return count;
}

void swapper_destroy(buffer_swapper_t *sw)
{
  swap_entry_t *current, *tmp;
  HASH_ITER(hh, sw->entries, current, tmp) {
    HASH_DEL(sw->entries, current);
    free(current);
  }

  while (!dllist_is_empty(&sw->disk_free_offsets)) {
    dllist_link *tmp = dllist_rem_head(&sw->disk_free_offsets);
    free_off_t *f = DLLIST_ELEMENT(tmp, free_off_t, link);
    free(f);
  }

  pthread_mutex_destroy(&sw->lock);

  // Delete filesystem entries for swap area
  DIR *dp;
  struct dirent *ep;
  if (!(dp = opendir(sw->dirname))) {
    return;
  }

  struct stat status;
  char filename[MAX_FILENAME_SIZE];
  while ((ep = readdir(dp))) {
    sprintf(filename, "%s%s", sw->dirname, ep->d_name);
    stat(filename, &status);
    if (S_ISDIR(status.st_mode)) {
      continue;
    }

    unlink(filename);
  }

  rmdir(sw->dirname);

  closedir(dp);
}

