#pragma once

#include <sys/types.h>

typedef enum {
    CRC_UNKNOWN,
    CRC32,
    CRC32C
} crc_mode_e;


/*
    Perform a CRC integrity on buf for buf_len bytes and compare against
    checksum.
    mode selects which CRC32 should be used.
*/
int perform_integrity_check(const uint8_t* buf,
                            size_t buf_len,
                            uint32_t checksum,
                            crc_mode_e mode);

/*
    Get the checksum for buf/buf_len
    mode selects which CRC to use.
     - CRC_UNKNOWN is illegal and will assert if used.
*/
uint32_t get_checksum(const uint8_t* buf,
                      size_t buf_len,
                      crc_mode_e mode);

