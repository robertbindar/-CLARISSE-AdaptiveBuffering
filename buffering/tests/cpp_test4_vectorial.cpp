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

#ifdef _BENCHMARKING
#include "benchmarking.h"
#endif

using namespace std;
using namespace std::chrono;

std::mutex g_lock;
uint32_t nr_consumers_finished = 0;
uint32_t nr_producers_finished = 0;

const char *input_file = NULL;

#define VEC_SIZE 4

void producer(cls_buffering_t *bufservice, uint32_t rank, uint32_t bufsize,
              uint32_t nprod)
{
    struct stat finfo;
    uint64_t file_size;

    int32_t fd = open(input_file, O_RDONLY);
    fstat(fd, &finfo);
    file_size = finfo.st_size;

    char *file_addr = (char *)mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);

    uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);

    uint64_t offsetv[VEC_SIZE];
    uint64_t countv[VEC_SIZE];

    auto elapsed_time = milliseconds::zero();
    uint32_t i = 0;
    cls_buf_handle_t handle;
    handle.global_descr = 0;

    char *buf = new char[bufsize];
    while (i < nrbufs) {
        handle.offset = i * bufsize;

        uint32_t size = 0;
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

        uint32_t offset = rank * (size / nprod);

        std::copy(file_addr + i * bufsize, file_addr + i * bufsize + bufsize, buf);

        uint32_t s = offset;
        uint32_t c = count / VEC_SIZE;

        // Populate offsets vectors
        for (uint32_t j = 0; j < VEC_SIZE; ++j) {
            offsetv[j] = s;
            s += c;
            countv[j] = c;
        }
        countv[VEC_SIZE - 1] = count - c * (VEC_SIZE - 1);

        auto start_time = steady_clock::now();
        cls_put_vector_all(bufservice, handle, offsetv, countv, VEC_SIZE, buf, nprod);
        auto end_time = steady_clock::now();

#ifdef _BENCHMARKING
        {
            std::lock_guard<mutex> guard(g_lock);
            print_counters(bufservice);
        }
#endif

        elapsed_time += duration_cast<milliseconds>(end_time - start_time);

        ++i;
    }
    delete [] buf;


    static auto avg_time = milliseconds::zero();
    static auto min_time = milliseconds::max();
    static auto max_time = milliseconds::zero();

    {
        std::lock_guard<mutex> guard(g_lock);

        avg_time += duration_cast<milliseconds>(elapsed_time);
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

    int32_t input = open(input_file, O_RDONLY);
    fstat(input, &finfo);
    file_size = finfo.st_size;
    close(input);

    char filename[100];
    sprintf(filename, "%s", "output");

    int32_t fd = open(filename, O_WRONLY | O_CREAT, S_IRWXU);
    ftruncate(fd, file_size);

    uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);

    auto elapsed_time = milliseconds::zero();

    uint32_t i = 0;
    cls_buf_handle_t handle;
    handle.global_descr = 0;

    uint64_t offsetv[VEC_SIZE];
    uint64_t countv[VEC_SIZE];

    char *data = new char[bufsize];
    while (i < nrbufs) {
        handle.offset = i * bufsize;

        uint32_t count = 0;
        if (file_size % bufsize && i == nrbufs - 1) {
            count = bufsize - (nrbufs * bufsize - file_size);
        } else {
            count = bufsize;
        }

        uint32_t size = count / ncons;
        if (rank == ncons - 1) {
            size = count - (ncons - 1) * (count / ncons);
        }

        uint32_t offset = rank * (count / ncons);

        uint32_t s = offset;
        uint32_t c = size / VEC_SIZE;

        // Populate offsets vectors
        for (uint32_t j = 0; j < VEC_SIZE; ++j) {
            offsetv[j] = s;
            s += c;
            countv[j] = c;
        }
        countv[VEC_SIZE - 1] = size - c * (VEC_SIZE - 1);

        auto start_time = steady_clock::now();
        cls_get_vector_all(bufservice, handle, offsetv, countv, VEC_SIZE, data, ncons);
        auto end_time = steady_clock::now();

#ifdef _BENCHMARKING
        {
            std::lock_guard<mutex> guard(g_lock);
            print_counters(bufservice);
        }
#endif

        elapsed_time += duration_cast<milliseconds>(end_time - start_time);

        for (uint32_t j = 0; j < VEC_SIZE; ++j) {
            lseek(fd, i * bufsize + offsetv[j], SEEK_SET);
            write(fd, data + offsetv[j], countv[j]);
        }
        ++i;
    }
    delete [] data;

    close(fd);

    static auto avg_time = milliseconds::zero();
    static auto min_time = milliseconds::max();
    static auto max_time = milliseconds::zero();

    std::lock_guard<mutex> guard(g_lock);

    avg_time += duration_cast<milliseconds>(elapsed_time);
    min_time = min(min_time, elapsed_time);
    max_time = max(max_time, elapsed_time);


    nr_consumers_finished++;
    if (nr_consumers_finished == ncons) {
        cerr << "Consumer average time: " << avg_time.count() / ncons << " ms" << endl;
        cerr << "Consumer minimum time: " << min_time.count() << " ms" << endl;
        cerr << "Consumer maximum time: " << max_time.count() << " ms" << endl;

        int passed = 1;
        int32_t input = open(input_file, O_RDONLY);
        void *input_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, input, 0);

        char file[100];
        sprintf(file, "%s", "output");

        int32_t output = open(file, O_RDONLY);
        void *output_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, output, 0);

        if (memcmp(input_addr, output_addr, file_size)) {
            passed = 0;
            cerr << "--Test " << __FILE__ << " failed: " << file << " does not match input\n";
        }

        close(output);
        munmap(output_addr, file_size);
        unlink(file);

        if (passed) {
            cerr << "++Test " << __FILE__ << " passed\n";
        }

        munmap(input_addr, file_size);
        close(input);
    }
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        cerr << "Usage ./test input_file nr_producers nr_consumers\n";
        return -1;
    }

    input_file = argv[1];
    uint32_t nr_producers = atoi(argv[2]);
    uint32_t nr_consumers = atoi(argv[3]);

    uint32_t bufsize = 1024;

    int32_t fd = open(input_file, O_RDONLY);
    struct stat finfo;
    fstat(fd, &finfo);
    uint32_t file_size = finfo.st_size;
    uint32_t nrbufs = file_size / bufsize + (file_size % bufsize != 0);
    close(fd);

    cls_buffering_t bufservice;
    cls_init_buffering(&bufservice, bufsize, nrbufs);

#ifdef _BENCHMARKING
    init_benchmarking(0, nr_consumers, nr_producers, 1);
#endif

    vector<std::thread> workers;

    for (uint32_t i = 0; i < nr_producers; ++i) {
        std::thread worker(producer, &bufservice, i, bufsize, nr_producers);
        workers.push_back(std::move(worker));
    }

    for (uint32_t i = 0; i < nr_consumers; ++i) {
        std::thread worker(consumer, &bufservice, i, bufsize, nr_consumers);
        workers.push_back(std::move(worker));
    }

#ifdef _BENCHMARKING
    //while (true) {
        //{
            //std::lock_guard<mutex> guard(g_lock);
            //if (nr_consumers_finished + nr_producers_finished == nr_consumers + nr_producers) {
                //break;
            //}
        //}

        //print_counters(&bufservice, nr_consumers, nr_producers);
        //std::this_thread::sleep_for(1ms);
    //}
#endif

    for (auto &t : workers) {
        t.join();
    }

    cls_destroy_buffering(&bufservice);

#ifdef _BENCHMARKING
    destroy_benchmarking();
#endif

    return 0;
}

