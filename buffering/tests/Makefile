CC = clang++-3.5
CFLAGS = -g -Wall -I ../../include -std=c++14 -O3
LDFLAGS = -L../../bin/ -lpthread -lbuffering -O3
EXEC = cpp_test1.bin cpp_test2.bin cpp_test3.bin

all: $(EXEC)

cpp_test1.bin: cpp_test1.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

cpp_test2.bin: cpp_test2.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

cpp_test3.bin: cpp_test3.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

