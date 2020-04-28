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
 *      Hurricane Lee <hurricane1026@gmail.com>
 */

#ifndef __DISKMGR_H__
#define __DISKMGR_H__

#include <stdint.h>
#include "util.h"

typedef struct disk_mgr
{
    char **disks;
    int ndisks;
} Mgr;

Mgr *mgr_create(const char **disks, int ndisks);
void mgr_destroy(Mgr *mgr);
ssize_t mgr_readlink(const char *path, char *buf, size_t bufsiz);

const char *mgr_base(Mgr *mgr);
const char *mgr_alloc(Mgr *mgr, const char *path);

#define mgr_unlink(X)  _mgr_unlink(X, __FILE__, __LINE__, __FUNCTION__)
void _mgr_unlink(const char *path, const char *file, int line, const char *func);

void mgr_rename(const char *oldpath, const char *newpath);

void mgr_stat(Mgr *mgr, uint64_t *total, uint64_t *avail);

static inline char *simple_basename(const char *path)
{
    char *p = (char*)path + strlen(path);
    while (*p != '/' && p >= path)
        --p;
    return ++p;
}

int mgr_getrealpath(const char *path, char *buf, size_t bufsiz);

#endif
