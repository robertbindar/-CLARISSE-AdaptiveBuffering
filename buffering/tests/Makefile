CC = clang++-3.6
CFLAGS = -g -Wall -I ../../include -std=c++14 -O3
LDFLAGS = -L../../bin/ -lpthread -lbuffering -O3
EXEC = test1 test2

all: $(EXEC)

test1: test1.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test2: test2.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

