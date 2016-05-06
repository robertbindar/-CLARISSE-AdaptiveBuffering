CC = gcc
CFLAGS = -g -Wall -std=gnu99 -I ../include -fsanitize=thread
LDFLAGS = -lpthread -fsanitize=thread
EXEC = libbuffering.so


all: $(EXEC)

$(EXEC): *.o
	$(CC) $(CFLAGS) $^ -shared -o $@ $(LDFLAGS)
%.o: %.c
	$(CC) $(CFLAGS) -c -fPIC $^

clean:
	-rm -f *.o  *~ $(EXEC)

