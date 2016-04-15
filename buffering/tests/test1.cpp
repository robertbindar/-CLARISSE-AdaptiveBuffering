#include <iostream>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <thread>
#include <sstream>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <chrono>
#include "cls_buffering.h"

using namespace std;

std::mutex g_lock;
uint32_t nr_consumers = 0;

void producer(cls_buffering_t *bufservice, uint32_t rank, uint32_t bufsize,
              uint32_t nprod)
{
    struct stat finfo;
    uint64_t file_size;

    int32_t fd = open("input", O_RDONLY);
    fstat(fd, &finfo);
    file_size = finfo.st_size;

    char *file_addr = (char *)mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);

    uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);
    uint32_t chunk = nrbufs / nprod;
    uint32_t begin = rank * chunk;
    if (rank == nprod - 1) {
        chunk = nrbufs - chunk * (nprod - 1);
    }

    auto start_time = chrono::steady_clock::now();

    uint32_t i = 0;
    cls_buf_handle_t handle;
    handle.global_descr = 0;

    while (i < chunk) {
        handle.offset = (begin + i) * bufsize;

        uint32_t count = 0;
        if (rank == nprod - 1 && file_size % bufsize && i == chunk - 1) {
            count = bufsize - (nrbufs * bufsize - file_size);
        } else {
            count = bufsize;
        }

        cls_put(bufservice, handle, 0, file_addr + (begin + i) * bufsize, count);

        ++i;
    }

    auto end_time = chrono::steady_clock::now();
    {
        std::lock_guard<mutex> guard(g_lock);
        cerr << "Producer " << rank << " time: " << chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count() << " ms" << endl;
    }

    munmap(file_addr, file_size);
    close(fd);
}

void consumer(cls_buffering_t *bufservice, uint32_t rank, uint32_t bufsize,
              uint32_t ncons)
{
    struct stat finfo;
    uint32_t file_size;

    int32_t input = open("input", O_RDONLY);
    fstat(input, &finfo);
    file_size = finfo.st_size;
    close(input);

    char filename[100];
    sprintf(filename, "%s%d", "output", rank);

    int32_t fd = open(filename, O_WRONLY | O_CREAT, S_IRWXU);
    ftruncate(fd, file_size);

    uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);

    auto start_time = chrono::steady_clock::now();

    uint32_t i = 0;
    cls_buf_handle_t handle;
    handle.global_descr = 0;

    while (i < nrbufs) {
        handle.offset = i * bufsize;

        uint32_t count = 0;
        if (file_size % bufsize && i == nrbufs - 1) {
            count = bufsize - (nrbufs * bufsize - file_size);
        } else {
            count = bufsize;
        }

        char *data = new char[bufsize];
        cls_get(bufservice, handle, 0, data, count, ncons);

        lseek(fd, i * bufsize, SEEK_SET);
        write(fd, data, count);
        ++i;
        delete [] data;
    }

    auto end_time = chrono::steady_clock::now();
close(fd);

    std::lock_guard<mutex> guard(g_lock);
    cerr << "Consumer " << rank << " time: " << chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count() << " ms" << endl;

    nr_consumers++;
    if (nr_consumers == ncons) {
        int passed = 0;
        int32_t input = open("input", O_RDONLY);
        void *input_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, input, 0);

        for (i = 0; i < ncons && passed >= 0; ++i) {
            char file[100];
            sprintf(file, "%s%d", "output", i);

            int32_t output = open(file, O_RDONLY);
            void *output_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, output, 0);

            if (memcmp(input_addr, output_addr, file_size)) {
                passed = -1;
                cerr << "--Test " << __FILE__ << " failed: " << file << " does not match input\n";
            }

            close(output);
            munmap(output_addr, file_size);
            unlink(file);
        }

        if (passed >= 0) {
            cerr << "++Test " << __FILE__ << " passed\n";
        }

        munmap(input_addr, file_size);
        close(input);
    }
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        cerr << "Usage ./test nr_producers nr_consumers\n";
        return -1;
    }

    uint32_t nr_producers = atoi(argv[1]);
    uint32_t nr_consumers = atoi(argv[2]);

    uint32_t bufsize = 64;
    uint32_t max_pool_size = 4096;
    cls_buffering_t bufservice;
    cls_init_buffering(&bufservice, bufsize, max_pool_size);

    vector<std::thread> workers;

    for (uint32_t i = 0; i < nr_producers; ++i) {
        std::thread worker(producer, &bufservice, i, bufsize, nr_producers);
        workers.push_back(std::move(worker));
    }

    for (uint32_t i = 0; i < nr_consumers; ++i) {
        std::thread worker(consumer, &bufservice, i, bufsize, nr_consumers);
        workers.push_back(std::move(worker));
    }

    for (auto &t : workers) {
        t.join();
    }

    cls_destroy_buffering(&bufservice);

    return 0;
}

