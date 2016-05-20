#pragma once

#include <mpi.h>

void server(MPI_Comm intercomm_producer, MPI_Comm intercomm_consumer, MPI_Comm intracomm);

void *producer_handler(void *arg);

void *consumer_handler(void *arg);

