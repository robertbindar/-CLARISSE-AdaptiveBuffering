CC = mpicc
CFLAGS = -g -I ../include -O3 -Wno-unused-result
LDFLAGS = -L../bin/ -lpthread -lbuffering
SIMULATION = producer_consumer_decoupling.c listener.c
EXEC = mpi_decoupled_filetransfer.bin mpi_p2p_filetransfer.bin mpi_alloc_ondemand_vs_async.bin mpi_ondemand_ideal_adaptive.bin

all: $(EXEC)

mpi_decoupled_filetransfer.bin: mpi_decoupled_filetransfer.c $(SIMULATION)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

mpi_p2p_filetransfer.bin: mpi_p2p_filetransfer.c
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

mpi_alloc_ondemand_vs_async.bin: mpi_alloc_ondemand_vs_async.c $(SIMULATION)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

mpi_ondemand_ideal_adaptive.bin: mpi_ondemand_ideal_adaptive.c $(SIMULATION)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

