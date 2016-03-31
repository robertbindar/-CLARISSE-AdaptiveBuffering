#pragma once
#include <limits.h>

typedef enum
{
  #undef ERROR
  #define ERROR_CODE(name, val) name = val

  #include "error_codes.h"

  #undef ERROR
} error_code;

void error_to_string(error_code err, char *retval);

#define STATUS_OK(s) ((s) == BUFFERING_SUCCESS)
#define STATUS_FAILED(s) ((s) != BUFFERING_SUCCESS)

#define HANDLE_ERR(expr, errcode) if ((expr)) { return (errcode); }
