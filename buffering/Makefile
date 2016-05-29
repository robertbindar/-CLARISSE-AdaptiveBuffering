CC = gcc
CFLAGS = -g -Wall -std=gnu11 -I ../include -O3
LDFLAGS = -lpthread -O3
EXEC = libbuffering.so


all: $(EXEC)

$(EXEC): *.o
	$(CC) $(CFLAGS) $^ -shared -o $@ $(LDFLAGS)
%.o: %.c
	$(CC) $(CFLAGS) -c -fPIC $^

clean:
	-rm -f *.o  *~ $(EXEC)

