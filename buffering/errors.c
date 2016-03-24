#include "errors.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <limits.h>
#include <stdint.h>

#define END_OF_ERRLIST INT_MAX

typedef struct
{
  error_code err;
  const char *err_name;
} error_pair_t;


#undef ERROR_CODE
#define ERROR_CODE(name, val) {name, #name}

const error_pair_t errors[] = {
  #include "error_codes.h"
};

#undef ERROR_CODE

void error_to_string(error_code err, char *retval)
{
  assert(retval);

  uint32_t i = 0;
  while (errors[i].err != END_OF_ERRLIST) {
    if (errors[i].err == err) {
      strcpy(retval, errors[i].err_name);
      return;
    }
    ++i;
  }

  strcpy(retval, "BUFFERING_UNKNOWN_ERROR");
}

