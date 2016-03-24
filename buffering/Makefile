CC = gcc
CFLAGS = -g -Wall -I ./ -O0
LDFLAGS = -lpthread
EXEC = libbuffering.so


all: $(EXEC)

$(EXEC): *.o
	$(CC) $(CFLAGS) $^ -shared -o $@ $(LDFLAGS)
%.o: %.c
	$(CC) $(CFLAGS) -c -fPIC $^

clean:
	-rm -f *.o  *~ $(EXEC)

