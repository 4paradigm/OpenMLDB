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

#ifndef __MFILE_H__
#define __MFILE_H__

#include <sys/mman.h>
#include <fcntl.h>

typedef struct
{
    int fd;
    size_t size;
    char *addr;
} MFile;

MFile *open_mfile(const char *path);
void close_mfile(MFile *f);
static inline void mfile_dontneed(MFile *f,  size_t pos, size_t *last_advise) {
    if (pos - *last_advise > (64<<20))
    {
        madvise(f->addr, pos, MADV_DONTNEED);
#if _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L
        posix_fadvise(f->fd, 0, pos, POSIX_FADV_DONTNEED);
#endif
        *last_advise = pos;
    }
}
static inline void file_dontneed(int fd,  size_t pos, size_t *last_advise) {
    if (pos - *last_advise > (8<<20))
    {
#if _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L
        posix_fadvise(fd, 0, pos -  (64<<10), POSIX_FADV_DONTNEED);
#endif
        *last_advise = pos;
    }
}

#endif
