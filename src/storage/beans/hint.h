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

#ifndef __HINT_H__
#define __HINT_H__

#include "mfile.h"
#include "htree.h"
#include "util.h"

#define NAME_IN_RECORD 2

typedef struct hint_record
{
    uint32_t ksize:8;
    uint32_t pos:24;
    int32_t version;
    uint16_t hash;
    char key[NAME_IN_RECORD]; // allign
} HintRecord;

typedef struct
{
    MFile *f;
    size_t size;
    char *buf;
} HintFile;

HintFile *open_hint(const char *path, const char *new_path);
void close_hint(HintFile *hint);
void scanHintFile(HTree *tree, int bucket, const char *path, const char *new_path);
void build_hint(HTree *tree, const char *path);
void write_hint_file(char *buf, int size, const char *path);
int count_deleted_record(HTree *tree, int bucket, const char *path, int *total, bool skipped);

#endif
