CC = clang++-3.5
CFLAGS = -g -Wall -I ../../include -std=c++14 -O3
LDFLAGS = -L../../bin/ -lpthread -lbuffering -O3
EXEC = cpp_test1.bin cpp_test2.bin cpp_test3.bin #cpp_test4_vectorial.bin cpp_test5_vectorial_coll.bin

all: $(EXEC)

cpp_test1.bin: cpp_test1.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

cpp_test2.bin: cpp_test2.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

cpp_test3.bin: cpp_test3.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

#cpp_test4_vectorial.bin: cpp_test4_vectorial.cpp
	#$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

#cpp_test5_vectorial_coll.bin: cpp_test5_vectorial_coll.cpp
	#$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f *.o  *~ $(EXEC)

