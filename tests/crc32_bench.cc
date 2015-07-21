#include <stdint.h>
#include <cstring>
#include <vector>
#include <iostream>
#include <random>
#include <sstream>
#include <iomanip>

#include "platform/platform.h"
#include "platform/crc32c.h"
#include "crc32.h"

typedef uint32_t (*crc32c_function)(const uint8_t* buf, size_t len);

static std::vector<std::string> column_heads(0);

void crc_results_banner() {
    column_heads.push_back("Data size (bytes)");
    column_heads.push_back("hash_crc32 ");
    column_heads.push_back("GiB/s   ");
    column_heads.push_back("libplatform ");
    column_heads.push_back("GiB/s   ");
    column_heads.push_back("libplatform vs hash_crc32 ");
    for (auto str : column_heads) {
        std::cout << str << ": ";
    }
    std::cout << std::endl;
}

std::string gib_per_sec(size_t test_size, hrtime_t t) {
    uint64_t one_sec = 1000000000;
    uint64_t how_many_per_sec = one_sec / t;
    double bytes_per_sec = test_size * how_many_per_sec;
    double gib_per_sec = bytes_per_sec/(1024.0*1024.0*1024.0);
    std::stringstream ss;
    ss << std::fixed << std::setprecision(3) << gib_per_sec;
    return ss.str();
}

void crc_results(size_t test_size,
                 std::vector<hrtime_t> &timings_cs,
                 std::vector<hrtime_t> &timings_pl) {
    hrtime_t avg_cs = 0, avg_pl = 0;
    for(auto duration : timings_cs) {
        avg_cs += duration;
    }
    for(auto duration : timings_pl) {
        avg_pl += duration;
    }
    avg_cs = avg_cs / timings_cs.size();
    avg_pl = avg_pl / timings_pl.size();

    double cs_pl = static_cast<double>(avg_cs) / static_cast<double>(avg_pl);

    std::cout.precision(2);
    std::vector<std::string> rows(0);
    rows.push_back(std::to_string(test_size));
    rows.push_back(std::to_string(avg_cs));
    rows.push_back(gib_per_sec(test_size, avg_cs));
    rows.push_back(std::to_string(avg_pl));
    rows.push_back(gib_per_sec(test_size, avg_pl));
    rows.push_back(std::to_string(cs_pl));

    for (int ii = 0; ii < column_heads.size(); ii++) {
        std::string spacer(column_heads[ii].length() - rows[ii].length(), ' ');
        std::cout << rows[ii] << spacer << ": ";
    }
    std::cout << std::endl;
}

void crc_bench_core_platform(const uint8_t* buffer,
                             size_t len,
                             int iterations,
                             std::vector<hrtime_t> &timings) {
    for (int i = 0; i < iterations; i++) {
        const hrtime_t start = gethrtime();
        crc32c(buffer, len, 0);
        const hrtime_t end = gethrtime();
        timings.push_back(end - start);
    }
}

void crc_bench_core_couchstore(const uint8_t* buffer,
                             size_t len,
                             int iterations,
                             std::vector<hrtime_t> &timings) {
    for (int i = 0; i < iterations; i++) {
        const hrtime_t start = gethrtime();
        hash_crc32(reinterpret_cast<const char*>(buffer), len);
        const hrtime_t end = gethrtime();
        timings.push_back(end - start);
    }
}

void crc_bench(size_t len,
               int iterations,
               int unalignment) {
    uint8_t* data = new uint8_t[len+unalignment];
    std::mt19937 twister(len);
    for (int data_index = 0; data_index < len; data_index++) {
        char data_value = twister() & 0xff;
        data[data_index] = data_value;
    }
    std::vector<hrtime_t> timings_cs, timings_pl;
    crc_bench_core_couchstore(data+unalignment, len, iterations, timings_cs);
    crc_bench_core_platform(data+unalignment, len, iterations, timings_pl);
    delete [] data;

    crc_results(len, timings_cs, timings_pl);

}

int main() {
    std::cout << "Timings in ns\n";
    crc_results_banner();
    for(size_t size = 32; size <= 8*(1024*1024); size = size * 4) {
        crc_bench(size, 1000, 0);
    }
    std::cout << std::endl;
    for(size_t size = 33; size <= 8*(1024*1024); size = size * 4) {
        crc_bench(size, 1000, 0);
    }
    std::cout << std::endl;
    for(size_t size = 33; size <= 8*(1024*1024); size = size * 4) {
        crc_bench(size % 2 == 0? size+1:size, 1000, 1);
    }
    return 0;
}