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
 *
 */

#ifndef __BITCASK_H__
#define __BITCASK_H__

#include <stdint.h>

#include "record.h"
#include "diskmgr.h"
#include "common.h"

#include "util.h"

typedef struct bitcask_t Bitcask;

Bitcask*   bc_open(const char *path, int depth, int pos, time_t before);
Bitcask*   bc_open2(Mgr *mgr, int depth, int pos, time_t before);
void       bc_scan(Bitcask *bc);
void       bc_flush(Bitcask *bc, unsigned int limit, int period);
void       bc_close(Bitcask *bc);
void       bc_merge(Bitcask *bc);
int        bc_optimize(Bitcask *bc, int limit);
DataRecord* bc_get(Bitcask *bc, const char *key, uint32_t *ret_pos, bool return_deleted);
bool       bc_set(Bitcask *bc, const char *key, char *value, size_t vlen, int flag, int version);
bool       bc_delete(Bitcask *bc, const char *key);
uint16_t   bc_get_hash(Bitcask *bc, const char *pos, unsigned int *count);
char*      bc_list(Bitcask *bc, const char *pos, const char *prefix);
uint32_t   bc_count(Bitcask *bc, uint32_t *curr);
void       bc_stat(Bitcask *bc, uint64_t *bytes);

#endif
