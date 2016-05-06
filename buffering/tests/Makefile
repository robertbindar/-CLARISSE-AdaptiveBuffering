CC = clang++-3.6
CFLAGS = -g -Wall -I ../../include -std=c++14 -O2
LDFLAGS = -L../../bin/ -lpthread -lbuffering
EXEC = test1 test2

all: $(EXEC)

test1: test1.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test2: test2.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

