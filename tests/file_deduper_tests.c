/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2014 Couchbase, Inc.
 *
 * @author Sarath Lakshman  <sarath@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "macros.h"
#include "../src/file_merger.h"
#include "file_tests.h"

#define N_FILES 4
#define MAX_RECORDS_PER_FILE 10000

typedef struct {
    int key;
    int fileno;
} test_record_t;

static int read_record(FILE *f, void **buffer, void *ctx)
{
    test_record_t *rec = (test_record_t *) malloc(sizeof(test_record_t));
    (void) ctx;

    if (rec == NULL) {
        return FILE_MERGER_ERROR_ALLOC;
    }

    if (fread(rec, sizeof(test_record_t), 1, f) != 1) {
        free(rec);
        if (feof(f)) {
            return 0;
        } else {
            return FILE_MERGER_ERROR_FILE_READ;
        }
    }

    *buffer = rec;

    return sizeof(test_record_t);
}

static file_merger_error_t write_record(FILE *f, void *buffer, void *ctx)
{
    (void) ctx;

    if (fwrite(buffer, sizeof(test_record_t), 1, f) != 1) {
        return FILE_MERGER_ERROR_FILE_WRITE;
    }

    return FILE_MERGER_SUCCESS;
}

static int compare_records(const void *rec1, const void *rec2, void *ctx)
{
    int ret;
    test_record_t *a, *b;
    (void) ctx;

    a = (test_record_t *) rec1;
    b = (test_record_t *) rec2;

    return a->key - b->key;

    return ret;
}

static void free_record(void *rec, void *ctx)
{
   (void) ctx;

   free(rec);
}

static size_t dedup_records(file_merger_record_t **records, size_t n, void *ctx)
{
    size_t max = 0;
    size_t i;
    (void) ctx;

    for (i = 1; i < n; i++) {
        if (records[max]->filenum < records[i]->filenum) {
            max = i;
        }
    }

    return max;
}

static void check_deduped_file(const char *file_path, int *expected_set, int len)
{
    FILE *f;
    test_record_t *rec;
    int record_size = 1;
    size_t i;
    unsigned long num_records = 0;

    f = fopen(file_path, "rb");
    assert(f != NULL);

    while (record_size > 0) {
        record_size = read_record(f, (void **) &rec, NULL);
        assert(record_size >= 0);

        if (record_size > 0) {
            if (rec->key % 40 == 0) {
                assert(rec->fileno == 4);
            } else if (rec->key % 20 == 0) {
                assert(rec->fileno == 3);
            } else if (rec->key % 10 == 0) {
               assert(rec->fileno == 2);
            } else {
                assert(rec->fileno == 1);
            }

            assert(expected_set[rec->key]);
            num_records++;
            free_record((void *) rec, NULL);
        }
    }

    /* Verify count */
    for (i = 0; i < len; i++) {
        if (expected_set[i]) {
            num_records--;
        }
    }

    assert(num_records == 0);

    fclose(f);
}


void file_deduper_tests(void)
{
    const char *source_files[N_FILES] = {
        "sorted_file_1.tmp",
        "sorted_file_2.tmp",
        "sorted_file_3.tmp",
        "sorted_file_4.tmp"
    };
    const char *dest_file = "merged_file.tmp";
    unsigned i, j;
    file_merger_error_t ret;
    test_record_t rec;
    int key;
    int multiples[] = {5, 10, 20, 40};
    int max_arr_size = 40 * MAX_RECORDS_PER_FILE + 1;
    int *expected_result = calloc(40 * MAX_RECORDS_PER_FILE + 1, sizeof(int));
    assert(expected_result != NULL);

    fprintf(stderr, "\nRunning file deduper tests...\n");

    for (i = 0; i < N_FILES; ++i) {
        FILE *f;

        remove(source_files[i]);
        f = fopen(source_files[i], "ab");
        assert(f != NULL);

        for (j = 0; j < MAX_RECORDS_PER_FILE; ++j) {
            key = multiples[i] * (j + 1);
            rec.key = key;
            rec.fileno = i + 1;
            assert(fwrite(&rec, sizeof(test_record_t), 1, f) == 1);
            expected_result[key] = 1;
        }

        fclose(f);
    }

    remove(dest_file);
    ret = merge_files(source_files, N_FILES,
                      dest_file,
                      read_record, write_record, NULL, compare_records,
                      dedup_records, free_record, 0, NULL);

    assert(ret == FILE_MERGER_SUCCESS);
    check_deduped_file(dest_file, expected_result, max_arr_size);

    for (i = 0; i < N_FILES; ++i) {
        remove(source_files[i]);
    }
    remove(dest_file);

    fprintf(stderr, "Running file deduper tests passed\n\n");
    free(expected_result);
}
