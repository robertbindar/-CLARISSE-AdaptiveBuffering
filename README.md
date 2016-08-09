# CLARISSE-AdaptiveBuffering

This project aims to increase the performance of a CLARISSE [1] environment by
providing an adaptive buffering module.


[1] http://www.arcos.inf.uc3m.es/~florin/clarisse/



## Adaptive buffering API


1.
buffer handle: identified by a global_descr and offset

```C
typedef struct _cls_buf_handle
{
  cls_size_t offset;
  uint32_t global_descr;
} cls_buf_handle_t;
```

2.
```C
error_code cls_get(cls_buffering_t *bufservice, const cls_buf_handle_t bh,
                   const cls_size_t offset, cls_byte_t *data, const cls_size_t count,
                   const cls_size_t nr_consumers);
```

- called by consumers
- blocks if the buffer has not been updated by all participants
- nr_consumers are allowed to read from the buffer concurrently
- can retrieve the buffer from memory or disk

3.
```C
error_code cls_put(cls_buffering_t *bufservice, const cls_buf_handle_t bh,
                   const cls_size_t offset, const cls_byte_t *data,
                   const cls_size_t count);
```

- called by producers
- writes data to a buffer, only one producer at a time can write the buffer
- creates the buffer if it does not exist
- if the memory is full save most-recently “used and updated by all participants” to disk


4.
```C
error_code cls_put_all(cls_buffering_t *bufservice, const cls_buf_handle_t bh,
                       const cls_size_t offset, const cls_byte_t *data,
                       const cls_size_t count, const uint32_t nr_participants);
```

- called by producers
- writes data to a buffer, the data is made available to the blocked consumers when all the participants are done
- nr_participants producers are allowed to write the buffer concurrently, the user must take care not to overlap the offsets
- creates the buffer if it does not exist
- if the memory is full, save most-recently “used and updated by all participants” to disk

5.
```C
error_code cls_put_vector(cls_buffering_t *bufservice, const cls_buf_handle_t bh,
                          const cls_size_t *offsetv, const cls_size_t *countv,
                          const cls_size_t vector_size, const cls_byte_t *data);
```

- called by producers
- writes into the buffer vector_size data chunks described by offsetv[] and countv[] args
- only one producer at a time can write the buffer
- creates the buffer if it does not exist
- if the memory is full, save most-recently “used and updated by all participants” to disk


6.
```C
error_code cls_put_vector_all(cls_buffering_t *bufservice, const cls_buf_handle_t bh,
                              const cls_size_t *offsetv, const cls_size_t *countv,
                              const cls_size_t vector_size, const cls_byte_t *data,
                              const uint32_t nr_participants);
```

- called by producers
- writes into the buffer vector_size data chunks described by offsetv[] and countv[] args
- nr_participants producers are allowed to write the buffer concurrently, the user must take care not to overlap the offsets
- creates the buffer if it does not exist
- if the memory is full, save most-recently “used and updated by all participants” to disk


7.
```C
error_code cls_get_vector(cls_buffering_t *bufservice, const cls_buf_handle_t bh,
                          const cls_size_t *offsetv, const cls_size_t *countv,
                          const cls_size_t vector_size, cls_byte_t *data,
                          const uint32_t nr_consumers);
```

- called by consumers
- reads vector_size chunks of data from the buffer described by the offsetv[] and countv[] arguments
- blocks if the buffer has not been updated by all participants
- nr_consumers are allowed to read from the buffer concurrently
- can retrieve the buffer from memory or disk

