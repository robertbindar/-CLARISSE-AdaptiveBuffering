CC = clang++-3.6
CFLAGS = -g -Wall -I ../../include -std=c++11 -fsanitize=thread
LDFLAGS = -L../../bin/ -lpthread -lbuffering -fsanitize=thread
EXEC = test1 test2

all: $(EXEC)

test1: test1.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test2: test2.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

