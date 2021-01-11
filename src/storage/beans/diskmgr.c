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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <libgen.h>
#include <zconf.h>

#include "diskmgr.h"

#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "const.h"

ssize_t mgr_readlink(const char *path, char *buf, size_t bufsiz)
{
    int n = readlink(path, buf, bufsiz);
    if (n < 0)
    {
        printf("readlink fail %s\n", path);
        return -1;
    }
    buf[n] = 0;
    if (strncmp(simple_basename(path), simple_basename(buf), bufsiz) != 0 )
    {
        printf("basename not match %s->%s\n", path, buf);
        return -2;
    }
    return n;
}

int mgr_getrealpath(const char *path, char *buf, size_t bufsiz)
{
    struct stat sb;
    if (stat(path, &sb) !=0)
        return -1;

    if ((sb.st_mode & S_IFMT) == S_IFLNK)
    {
        if (mgr_readlink(path, buf, bufsiz) <= 0)
            return -1;
    }
    else
        strcpy(buf, path);
    return 0;
}

Mgr *mgr_create(const char **disks, int ndisks)
{
    char *cwd = getcwd(NULL, 0);
    Mgr *mgr = (Mgr*) beans_safe_malloc(sizeof(Mgr));
    mgr->ndisks = ndisks;
    mgr->disks = (char**)beans_safe_malloc(sizeof(char*) * ndisks);
    int i;
    for (i = 0; i < ndisks; i++)
    {
        if (0 != access(disks[i], F_OK) && 0 != mkdir(disks[i], 0755))
        {
            printf("access %s failed\n", disks[i]);
            free(mgr->disks);
            free(mgr);
            free(cwd);
            return NULL;
        }
        if (disks[i][0] == '/')
        {
            mgr->disks[i] = strdup(disks[i]);
        }
        else
        {
            mgr->disks[i] =  (char*)beans_safe_malloc(strlen(disks[i]) + strlen(cwd) + 2);
            sprintf(mgr->disks[i], "%s/%s", cwd, disks[i]);  //safe
        }
    }
    free(cwd);
    return mgr;
}

void mgr_destroy(Mgr *mgr)
{
    int i=0;
    for (i = 0; i< mgr->ndisks; i++)
    {
        free(mgr->disks[i]);
    }
    free(mgr->disks);
    free(mgr);
}

const char *mgr_base(Mgr *mgr)
{
    return mgr->disks[0];
}

static uint64_t
get_disk_avail(const char *path, uint64_t *total)
{
    struct statvfs stat;
    int r = statvfs(path, &stat);
    if (r != 0)
    {
        return 0ULL;
    }
    if (total != NULL)
    {
        *total = stat.f_blocks * stat.f_frsize;
    }
    return stat.f_bavail * stat.f_frsize;
}

const char *mgr_alloc(Mgr *mgr, const char *name)
{
    if (mgr->ndisks == 1)
    {
        return mgr->disks[0];
    }
    uint64_t maxa= 0;
    int maxi = 0, i;
    char path[MAX_PATH_LEN];
    struct stat sb;
    for (i = 0; i< mgr->ndisks; i++)
    {
        safe_snprintf(path, MAX_PATH_LEN, "%s/%s", mgr->disks[i], name);
        if (lstat(path, &sb) == 0 && (sb.st_mode & S_IFMT) == S_IFREG)
        {
            return mgr->disks[i];
        }
        uint64_t avail = get_disk_avail(mgr->disks[i], NULL);
        if (avail > maxa || (avail == maxa && (rand() & 1) == 1) )
        {
            maxa = avail;
            maxi = i;
        }
    }
    if (maxi != 0)
    {
        // create symlink
        char target[MAX_PATH_LEN];
        safe_snprintf(target, MAX_PATH_LEN, "%s/%s", mgr->disks[maxi], name);
        safe_snprintf(path, MAX_PATH_LEN, "%s/%s", mgr->disks[0], name);
        if (lstat(path, &sb) == 0)
        {
            unlink(path);
        }
        if (symlink(target, path) != 0)
        {
            printf("create symlink failed: %s -> %s\n", path, target);
            exit(1);
        }
    }
    return mgr->disks[maxi];
}

void _mgr_unlink(const char *path, const char *file, int line, const char *func)
{
    struct stat sb;
    if (0 != lstat(path, &sb))
        return;
    printf("mgr_unlink %s, in %s (%s:%i)\n", path, func, file, line);
    if ((sb.st_mode & S_IFMT) == S_IFLNK)
    {
        char buf[MAX_PATH_LEN];
        int n = mgr_readlink(path, buf, MAX_PATH_LEN);
        if (n > 0)
        {
            unlink(buf);
        }
    }
    unlink(path);
}


//caller guarantee newpath not exist
void mgr_rename(const char *oldpath, const char *newpath)
{
    printf("mgr_rename %s -> %s\n", oldpath, newpath);
    struct stat sb;
    char ropath[MAX_PATH_LEN];
    char rnpath[MAX_PATH_LEN];
    if (lstat(oldpath, &sb) == 0 && (sb.st_mode & S_IFMT) == S_IFLNK)
    {
        int n = mgr_readlink(oldpath, ropath, MAX_PATH_LEN);
        if (n > 0)
        {
            char *ropath_dup = strdup(ropath);
            safe_snprintf(rnpath, MAX_PATH_LEN, "%s/%s", dirname(ropath_dup), simple_basename(newpath));
            free(ropath_dup);

            if (symlink(rnpath, newpath) != 0)
            {
                printf("symlink failed: %s -> %s, err: %s, exit!\n", rnpath, newpath, strerror(errno));
                exit(-1);
            }
            printf("mgr_rename real %s -> %s\n", ropath, rnpath);
            if (rename(ropath, rnpath) != 0)
            {
                printf("rename failed: %s -> %s, err: %s, exit!\n", ropath, rnpath, strerror(errno));
                exit(-1);
            };
            unlink(oldpath);
        }
    }
    else
    {
        if (rename(oldpath, newpath) != 0)
        {
            printf("rename failed: %s -> %s, err:%s, exit!\n", oldpath, newpath, strerror(errno));
            exit(-1);
        };
    }
}

void mgr_stat(Mgr *mgr, uint64_t *total, uint64_t *avail)
{
    int i=0;
    *total = 0;
    *avail = 0;
    for (i = 0; i< mgr->ndisks; i++)
    {
        uint64_t t = 0;
        *avail += get_disk_avail(mgr->disks[i], &t);
        *total += t;
    }
}
