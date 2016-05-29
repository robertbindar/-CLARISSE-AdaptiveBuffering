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

error_code swapper_init(buffer_swapper_t *sw, uint64_t bufsize)
{
  sprintf(sw->dirname, "%s", ".swaparea/");
  sw->bufsize = bufsize;
  sw->entries = NULL;
  sw->entries_count = 0;
  HANDLE_ERR(pthread_mutex_init(&sw->lock, NULL), BUFSWAPPER_LOCK_ERR);
  dllist_init(&sw->disk_free_offsets);

  HANDLE_ERR(mkdir(sw->dirname, S_IRWXU) < 0, BUFSWAPPER_SYSCALL_ERR);

  return BUFFERING_SUCCESS;
}

error_code swapper_swapin(buffer_swapper_t *sw, cls_buf_t *buf)
{
  char filename[MAX_FILENAME_SIZE];
  sprintf(filename, "%s%" PRIu32, sw->dirname, buf->handle.global_descr);

  HANDLE_ERR(pthread_mutex_lock(&sw->lock), BUFSWAPPER_LOCK_ERR);

  int32_t fd = open(filename, O_RDONLY);
  HANDLE_ERR(fd < 0, BUFSWAPPER_BADIO_ERR);

  swap_entry_t *found;
  HASH_FIND_PTR(sw->entries, &buf, found);

  HANDLE_ERR(lseek(fd, found->file_offset, SEEK_SET) < 0, BUFSWAPPER_BADIO_ERR);

  HANDLE_ERR(read(fd, buf->data, sw->bufsize) != sw->bufsize, BUFSWAPPER_BADIO_ERR);

  HASH_DEL(sw->entries, found);
  sw->entries_count--;

  free_off_t *f = malloc(sizeof(free_off_t));
  HANDLE_ERR(!f, BUFSWAPPER_BADALLOC_ERR);

  f->offset = found->file_offset;
  dllist_iat(&sw->disk_free_offsets, &f->link);

  free(found);
  HANDLE_ERR(pthread_mutex_unlock(&sw->lock), BUFSWAPPER_LOCK_ERR);

  HANDLE_ERR(close(fd) < 0, BUFSWAPPER_BADIO_ERR);

  return BUFFERING_SUCCESS;
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

error_code swapper_swapout(buffer_swapper_t *sw, cls_buf_t *buf)
{
  swap_entry_t *entry = calloc(1, sizeof(swap_entry_t));
  HANDLE_ERR(!entry, BUFSWAPPER_BADALLOC_ERR);

  char filename[MAX_FILENAME_SIZE];
  sprintf(filename, "%s%" PRIu32, sw->dirname, buf->handle.global_descr);

  HANDLE_ERR(pthread_mutex_lock(&sw->lock), BUFSWAPPER_LOCK_ERR);

  int32_t fd = open(filename, O_RDWR | O_CREAT, S_IRWXU);
  HANDLE_ERR(fd < 0, BUFSWAPPER_BADIO_ERR);

  if (!dllist_is_empty(&sw->disk_free_offsets)) {
    dllist_link *tmp = dllist_rem_head(&sw->disk_free_offsets);
    free_off_t *f = DLLIST_ELEMENT(tmp, free_off_t, link);
    entry->file_offset = lseek(fd, f->offset, SEEK_SET);
    free(f);
  } else {
    entry->file_offset = lseek(fd, 0, SEEK_END);
  }

  HANDLE_ERR(write(fd, buf->data, sw->bufsize) != sw->bufsize, BUFSWAPPER_BADIO_ERR);
  HANDLE_ERR(close(fd) < 0, BUFSWAPPER_BADIO_ERR);

  entry->buf = buf;
  HASH_ADD_PTR(sw->entries, buf, entry);
  sw->entries_count++;

  HANDLE_ERR(pthread_mutex_unlock(&sw->lock), BUFSERVICE_LOCK_ERR);

  return BUFFERING_SUCCESS;
}

uint64_t swapper_getcount(buffer_swapper_t *sw)
{
  pthread_mutex_lock(&sw->lock);
  uint64_t count = sw->entries_count;
  pthread_mutex_unlock(&sw->lock);

  return count;
}

error_code swapper_destroy(buffer_swapper_t *sw)
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

  HANDLE_ERR(pthread_mutex_destroy(&sw->lock), BUFSWAPPER_LOCK_ERR);

  // Delete filesystem entries for swap area
  DIR *dp;
  struct dirent *ep;
  HANDLE_ERR(!(dp = opendir(sw->dirname)), BUFSWAPPER_BADIO_ERR);

  struct stat status;
  char filename[MAX_FILENAME_SIZE];
  while ((ep = readdir(dp))) {
    sprintf(filename, "%s%s", sw->dirname, ep->d_name);
    stat(filename, &status);
    if (S_ISDIR(status.st_mode)) {
      continue;
    }

    HANDLE_ERR(unlink(filename) < 0, BUFSWAPPER_BADIO_ERR);
  }

  HANDLE_ERR(rmdir(sw->dirname), BUFSWAPPER_BADIO_ERR);

  return BUFFERING_SUCCESS;
}

