/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <platform/crc32c.h>
#include "crc32.h"
#include "internal.h"

#include <iostream>

uint32_t crc32(const uint8_t* buffer, size_t len, crc32_method crc32) {
    switch(crc32) {
        case CRC32C: {
            return crc32c(buffer, len, 0);
        }
        case CRC32: {
            return hash_crc32(reinterpret_cast<const char*>(buffer), len);
        }
        case UNKNOWN: {
            return hash_crc32(reinterpret_cast<const char*>(buffer), len);
        }
        default: {
             return 0;
        }
    }
}