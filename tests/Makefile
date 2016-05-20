CC = mpicc
CFLAGS = -g -I ../include -O0
LDFLAGS = -L../bin/ -lpthread -lbuffering
SIMULATION = ../simulation/producer_consumer_decoupling.c ../simulation/listener.c ../simulation/server.c
EXEC = test_NM1_filetransfer1 test_NM1_filetransfer2_collective

all: $(EXEC)

test_NM1_filetransfer1: test_NM1_filetransfer1.c $(SIMULATION)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test_NM1_filetransfer2_collective: test_NM1_filetransfer2_collective.c $(SIMULATION)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

