/*
 *  Beansdb - A high available distributed key-value storage system:
 *
 *      http://beansdb.googlecode.com
 *
 *  Copyright 2010 Douban Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Davies Liu <davies.liu@gmail.com>
 *      Hurricane Lee <hurricane1026@gmail.com>
 *
 */

#ifndef __RECORD_H__
#define __RECORD_H__

#include <stdio.h>
#include <time.h>

#include "htree.h"
#include "util.h"
#include "diskmgr.h"


typedef struct data_record
{
    char *value;
    union
    {
        bool free_value;    // free value or not
        uint32_t crc;
    };
    int32_t tstamp;
    int32_t flag;
    int32_t version;
    uint32_t ksz;
    uint32_t vsz;
    char key[0];
} DataRecord;

typedef bool (*RecordVisitor)(DataRecord *r, void *arg1, void *arg2);

uint32_t gen_hash(char *buf, int size);

char* record_value(DataRecord *r);
void free_record(DataRecord **r);

// on bad record, return NULL and set *fail_reason to one of these
#define BAD_REC_SIZE  1
#define BAD_REC_END  2
#define BAD_REC_CRC  3
#define BAD_REC_DECOMPRESS 4
DataRecord* decode_record(char *buf, uint32_t size, bool decomp, const char *path, uint32_t pos, const char *key, bool do_logging, int *fail_reason);

char* encode_record(DataRecord *r, unsigned int *size);
DataRecord* read_record(FILE *f, bool decomp, const char *path, const char *key);
DataRecord* fast_read_record(int fd, off_t offset, bool decomp, const char *path, const char *key);

void scanDataFile(HTree *tree, int bucket, const char *path, const char *hintpath);
void scanDataFileBefore(HTree *tree, int bucket, const char *path, time_t before);
int optimizeDataFile(HTree *tree, Mgr *mgr, int bucket, const char *path, const char *hintpath,
        int last_bucket, const char *lastdata, const char *lasthint_real, uint32_t max_data_size,
        bool skipped, bool isnewfile, uint32_t *deleted_bytes);
void visit_record(const char *path, RecordVisitor visitor, void *arg1, void *arg2, bool decomp);

#endif
