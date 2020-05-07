/*
 *  Beansdb - A high available distributed key-value storage system:
 *
 *      http://beansdb.googlecode.com
 *
 *  Copyright 2009 Douban Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Davies Liu <davies.liu@gmail.com>
 */

#ifndef __HSTORE_H__
#define __HSTORE_H__

#include <time.h>
#include <stdint.h>

#include "util.h"

typedef struct t_hstore HStore;

HStore* hs_open(char *path, int height, time_t before, int scan_threads);
void    hs_flush(HStore *store, unsigned int limit, int period);
void    hs_close(HStore *store);
char*   hs_get(HStore *store, char *key, unsigned int *vlen, uint32_t *flag);
bool    hs_set(HStore *store, char *key, char *value, unsigned int vlen, uint32_t flag, int version);
bool    hs_append(HStore *store, char *key, char *value, unsigned int vlen);
int64_t hs_incr(HStore *store, char *key, int64_t value);
bool    hs_delete(HStore *store, char *key);
uint64_t hs_count(HStore *store, uint64_t *curr);
void    hs_stat(HStore *store, uint64_t *total, uint64_t *avail);
int     hs_optimize(HStore *store, long limit, char *tree);
int     hs_optimize_stat(HStore *store);
#endif
