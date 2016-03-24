CC = mpicc
MPIEXEC = mpiexec
CFLAGS = -g -Wall -I ../include -O0
LDFLAGS = -L../bin/ -lpthread -lbuffering
EXEC = producer_consumer_decoupling


all: $(EXEC)

$(EXEC): *.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)
%.o: %.c
	$(CC) $(CFLAGS) -c $^
run:
	BUFFERING_NUMBER_OF_PRODUCERS=4 \
    BUFFERING_NUMBER_OF_CONSUMERS=4 \
    BUFFERING_NUMBER_OF_SERVERS=1 \
    $(MPIEXEC) -n 9 ./$(EXEC)
debug:
	BUFFERING_NUMBER_OF_PRODUCERS=1 \
    BUFFERING_NUMBER_OF_CONSUMERS=1 \
    BUFFERING_NUMBER_OF_SERVERS=1 \
    $(MPIEXEC) -n 3 xterm -e gdb ./$(EXEC)


clean:
	-rm -f *.o  *~ $(EXEC)

