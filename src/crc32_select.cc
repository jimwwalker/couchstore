/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <platform/crc32c.h>
#include "crc32.h"
#include "internal.h"

#include <iostream>

uint32_t crc32(const uint8_t* buffer, size_t len, crc32_method crc32) {
    switch(crc32) {
        case CRC32C: {
            std::cout << "new" << std::endl;
            return crc32c(buffer, len, 0);
        }
        case CRC32: {
            std::cout << "old" << std::endl;
            return hash_crc32(reinterpret_cast<const char*>(buffer), len);
        }
        case UNKNOWN: {
            std::cout << "EH?" <<std::endl;
            return hash_crc32(reinterpret_cast<const char*>(buffer), len);
        }
        default: {
             std::cout << "BOOM?" <<std::endl;
             return 0;
        }
    }
}