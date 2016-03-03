#pragma once

#include "mpi.h"

void server(MPI_Comm intercomm_producer, MPI_Comm intercomm_consumer);

void *producer_handler(void *arg);

void *consumer_handler(void *arg);

