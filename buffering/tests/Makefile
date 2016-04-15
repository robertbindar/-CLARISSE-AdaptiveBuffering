CC = g++
CFLAGS = -g -Wall -I ../../include -O0 -std=c++11
LDFLAGS = -L../../bin/ -lpthread -lbuffering
EXEC = test1

all: $(EXEC)

test1: test1.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

