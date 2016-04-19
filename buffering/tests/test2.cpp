#include <iostream>
#include <algorithm>
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
using namespace std::chrono;

std::mutex g_lock;
uint32_t nr_consumers_finished = 0;
uint32_t nr_producers_finished = 0;

void producer(cls_buffering_t *bufservice, uint32_t rank, uint32_t bufsize,
              uint32_t nprod)
{
    struct stat finfo;
    uint64_t file_size;

    int32_t fd = open("input", O_RDONLY);
    fstat(fd, &finfo);
    file_size = finfo.st_size;

    char *file_addr = (char *)mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);

    auto start_time = steady_clock::now();

    uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);

    uint32_t i = 0;
    cls_buf_handle_t handle;
    handle.global_descr = 0;

    // All the producers will collectively write each buffer
    while (i < nrbufs) {
        uint32_t size;
        if (file_size % bufsize && i == nrbufs - 1) {
            size = bufsize - (nrbufs * bufsize - file_size);
        } else {
            size = bufsize;
        }

        uint32_t count = size / nprod;
        if (rank == nprod - 1) {
            // The last producer might get a bigger chunk
            count = size - (nprod - 1) * (size / nprod);
        }

        handle.offset = i * bufsize;
        uint32_t offset = rank * (size / nprod);

        cls_put_all(bufservice, handle, offset, file_addr + i * bufsize + offset, count, nprod);

        ++i;
    }

    auto end_time = steady_clock::now();

    static auto avg_time = milliseconds::zero();
    static auto min_time = milliseconds::max();
    static auto max_time = milliseconds::zero();

    auto elapsed_time = duration_cast<milliseconds>(end_time - start_time);

    {
        std::lock_guard<mutex> guard(g_lock);

        avg_time += duration_cast<milliseconds>(end_time - start_time);
        min_time = min(min_time, elapsed_time);
        max_time = max(max_time, elapsed_time);

        nr_producers_finished++;
        if (nr_producers_finished == nprod) {
            cerr << "Producer average time: " << avg_time.count() / nprod << " ms" << endl;
            cerr << "Producer minimum time: " << min_time.count() << " ms" << endl;
            cerr << "Producer maximum time: " << max_time.count() << " ms" << endl;
        }
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

    auto start_time = steady_clock::now();

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

    auto end_time = steady_clock::now();
close(fd);

    static auto avg_time = milliseconds::zero();
    static auto min_time = milliseconds::max();
    static auto max_time = milliseconds::zero();

    auto elapsed_time = duration_cast<milliseconds>(end_time - start_time);

    std::lock_guard<mutex> guard(g_lock);

    avg_time += duration_cast<milliseconds>(end_time - start_time);
    min_time = min(min_time, elapsed_time);
    max_time = max(max_time, elapsed_time);


    nr_consumers_finished++;
    if (nr_consumers_finished == ncons) {
        cerr << "Consumer average time: " << avg_time.count() / ncons << " ms" << endl;
        cerr << "Consumer minimum time: " << min_time.count() << " ms" << endl;
        cerr << "Consumer maximum time: " << max_time.count() << " ms" << endl;

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

    uint32_t bufsize = 128;
    uint32_t max_pool_size = 4096;

    int32_t fd = open("input", O_RDONLY);
    struct stat finfo;
    fstat(fd, &finfo);
    uint32_t file_size = finfo.st_size;
    uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);
    close(fd);

    cls_buffering_t bufservice;
    cls_init_buffering(&bufservice, bufsize, nrbufs);

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

