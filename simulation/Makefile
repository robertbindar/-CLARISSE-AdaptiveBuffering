CC = mpicc
CFLAGS = -g -I ../include -O3
LDFLAGS = -L../bin/ -lpthread -lbuffering
SIMULATION = producer_consumer_decoupling.c listener.c
EXEC = mpi_test_filetransfer1.bin

all: $(EXEC)

mpi_test_filetransfer1.bin: mpi_test_filetransfer1.c $(SIMULATION)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

