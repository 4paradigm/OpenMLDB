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

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <pthread.h>
#include <math.h>
#include <errno.h>
#include <time.h>

#include "bitcask.h"
#include "htree.h"
#include "hstore.h"
#include "diskmgr.h"
#include "fnv1a.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "const.h"
#include <unistd.h>

#define NUM_OF_MUTEX 37
#define MAX_PATHS 20
const int APPEND_FLAG  = 0x00000100;
const int INCR_FLAG    = 0x00000204;

struct t_hstore
{
    int height, count;
    time_t before;
    int scan_threads;
    int op_start, op_end, op_laststat, op_limit; // for optimization
    Mgr *mgr;
    pthread_mutex_t locks[NUM_OF_MUTEX];
    Bitcask *bitcasks[];
};

static inline int get_index(HStore *store, char *key)
{
    if (store->height == 0) return 0;
    uint32_t h = fnv1a(key, strlen(key));
    return h >> ((8 - store->height) * 4);
}

static inline pthread_mutex_t * get_mutex(HStore *store, char *key)
{
    uint32_t i = fnv1a(key, strlen(key)) % NUM_OF_MUTEX;
    return &store->locks[i];
}


// scan
static int scan_completed = 0;
static pthread_mutex_t scan_lock;
static pthread_cond_t  scan_cond;

typedef void (*BC_FUNC)(Bitcask *bc);

struct scan_args
{
    HStore *store;
    int index;
    BC_FUNC func;
};

static void *scan_thread(void *_args)
{
    struct scan_args *args = (struct scan_args*)_args;
    HStore *store = args->store;
    int i, index = args->index;
    for (i = 0; i < store->count; i++)
    {
        if (i % store->scan_threads == index)
        {
            args->func(store->bitcasks[i]);
        }
    }

    pthread_mutex_lock(&scan_lock);
    scan_completed++;
    pthread_cond_signal(&scan_cond);
    pthread_mutex_unlock(&scan_lock);

    printf("thread %d completed\n", index);
    return NULL;
}

static void parallelize(HStore *store, BC_FUNC func)
{
    scan_completed = 0;
    pthread_mutex_init(&scan_lock, NULL);
    pthread_cond_init(&scan_cond, NULL);

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    int i, ret;
    pthread_t *thread_ids = (pthread_t*)safe_malloc(sizeof(pthread_t) * store->scan_threads);
    struct scan_args *args = (struct scan_args *) safe_malloc(sizeof(struct scan_args) * store->scan_threads);
    for (i = 0; i < store->scan_threads; i++)
    {
        args[i].store = store;
        args[i].index = i;
        args[i].func = func;
        if ((ret = pthread_create(thread_ids + i, &attr, scan_thread, args + i)) != 0)
        {
            printf("Can't create thread: %s\n", strerror(ret));
            exit(1);
        }
    }

    pthread_mutex_lock(&scan_lock);
    while (scan_completed < store->scan_threads)
    {
        pthread_cond_wait(&scan_cond, &scan_lock);
    }
    pthread_mutex_unlock(&scan_lock);

    for (i = 0; i < store->scan_threads; i++)
    {
        pthread_join(thread_ids[i], NULL);
        pthread_detach(thread_ids[i]);
    }
    free(thread_ids);
    free(args);
}

HStore *hs_open(char *path, int height, time_t before, int scan_threads)
{
    if (NULL == path) return NULL;
    if (height < 0 || height > 3)
    {
        printf("invalid db height: %d\n", height);
        return NULL;
    }
    if (before != 0)
    {
        if (before<0)
        {
            printf("invalid time:%ld\n", before);
            return NULL;
        }
        else
        {
            printf("serve data modified before %s\n", ctime(&before));
        }
    }

    char *paths[20], *rpath = path;
    int npath = 0;
    printf("path is %s\n", path);
    printf("rpath is %s\n", rpath);
    while ((paths[npath] = strsep(&rpath, ",:")) != NULL)
    {
        if (npath >= MAX_PATHS) return NULL;
        printf("%d path %s\n", npath, paths[npath]);
        path = paths[npath];
        if (strlen(path) > MAX_HOME_PATH_LEN)
        {
            printf("path %s logger then %d\n", path, MAX_HOME_PATH_LEN);
            return NULL;
        }
        if (0 != access(path, F_OK) && 0 != mkdir(path, 0755))
        {
            printf("mkdir %s failed\n", path);
            return NULL;
        }
        if (height > 1)
        {
            // try to mkdir
            HStore *s = hs_open(path, height - 1, 0, 0);
            if (s == NULL)
            {
                return NULL;
            }
            hs_close(s);
        }

        ++npath;
    }

    int i, j, count = 1 << (height * 4);
    printf("current height %d, count is %d\n", height, count);
    HStore *store = (HStore*) safe_malloc(sizeof(HStore) + sizeof(Bitcask*) * count);
    if (!store) return NULL;
    memset(store, 0, sizeof(HStore) + sizeof(Bitcask*) * count);
    store->height = height;
    store->count = count;
    store->before = before;
    store->scan_threads = scan_threads;
    store->op_start = 0;
    store->op_end = 0;
    store->op_limit = 0;
    store->mgr = mgr_create((const char**)paths, npath);
    if (store->mgr == NULL)
    {
        free(store);
        return NULL;
    }
    for (i = 0; i < NUM_OF_MUTEX; i++)
    {
        pthread_mutex_init(&store->locks[i], NULL);
    }

    char *buf[20] = {0};
    for (i = 0; i < npath; i++)
    {
        buf[i] = (char*)safe_malloc(MAX_PATH_LEN);
    }
    for (i = 0; i<count; i++)
    {
        for (j = 0; j < npath; j++)
        {
            path = paths[j];
            switch(height)
            {
            case 0:
                safe_snprintf(buf[j], MAX_PATH_LEN, "%s", path);
                break;
            case 1:
                safe_snprintf(buf[j], MAX_PATH_LEN, "%s/%x", path, i);
                break;
            case 2:
                safe_snprintf(buf[j], MAX_PATH_LEN, "%s/%x/%x", path, i>>4, i & 0xf);
                break;
            case 3:
                safe_snprintf(buf[j], MAX_PATH_LEN, "%s/%x/%x/%x", path, i>>8, (i>>4)&0xf, i&0xf);
                break;
            }
        }
        Mgr *mgr = mgr_create((const char**)buf, npath);
        if (mgr == NULL) return NULL;
        store->bitcasks[i] = bc_open2(mgr, height, i, before);
    }
    for (i = 0; i < npath; i++)
    {
        free(buf[i]);
    }

    if (store->scan_threads > 1 && count > 1)
    {
        parallelize(store, bc_scan);
    }
    else
    {
        for (i = 0; i < count; i++)
        {
            bc_scan(store->bitcasks[i]);
        }
    }

    return store;
}

void hs_flush(HStore *store, unsigned int limit, int period)
{
    if (!store) return;
    if (store->before > 0) return;
    int i;
    for (i = 0; i < store->count; i++)
    {
        bc_flush(store->bitcasks[i], limit, period);
    }
}

void hs_close(HStore *store)
{
    int i;
    if (!store) return;
    // stop optimizing
    store->op_start = store->op_end = 0;

    if (store->scan_threads > 1 && store->count > 1)
    {
        parallelize(store, bc_close);
    }
    else
    {
        for (i = 0; i < store->count; i++)
        {
            bc_close(store->bitcasks[i]);
        }
    }
    mgr_destroy(store->mgr);
    free(store);
}

static uint16_t hs_get_hash(HStore *store, char *pos, uint32_t *count)
{
    if (strlen(pos) >= (unsigned int)(store->height))
    {
        pos[store->height] = 0;
        int index = strtol(pos, NULL, 16);
        return bc_get_hash(store->bitcasks[index], "@", count);
    }
    else
    {
        uint16_t i, hash=0;
        *count = 0;
        char pos_buf[255];
        for (i = 0; i < 16; ++i)
        {
            /*int h,c;*/
            uint16_t h;
            uint32_t c;
            safe_snprintf(pos_buf, 255, "%s%x", pos, i);
            h = hs_get_hash(store, pos_buf, &c);
            hash *= 97;
            hash += h;
            *count += c;
        }
        return hash;
    }
}

static char *hs_list(HStore *store, char *key)
{
    char *prefix = NULL;
    int p = 0, pos = strlen(key);
    while (p < pos)
    {
        if (key[p] == ':')
        {
            prefix = &key[p+1];
            break;
        }
        p++;
    }
    if (p > 8) return NULL;

    if (p >= store->height)
    {
        char buf[20] = {0};
        safe_memcpy(buf, 20, key, store->height);
        int index = strtol(buf, NULL, 16);
        safe_memcpy(buf, 20, key, p);
        return bc_list(store->bitcasks[index], buf + store->height, prefix);
    }
    else
    {
        int i, bsize = 1024, used = 0;
        char *buf = (char*)try_malloc(bsize);
        if (!buf) return NULL;
        for (i = 0; i < 16; ++i)
        {
            char pos_buf[255];
            safe_memcpy(pos_buf, 255, key, p);
            safe_snprintf(pos_buf + p, 255 - p , "%x", i);
            uint32_t hash, count;
            hash = hs_get_hash(store, pos_buf, &count);
            used += safe_snprintf(buf + used, bsize - used, "%x/ %u %u\n", i, hash & 0xffff, count);
        }
        return buf;
    }
}

char *hs_get(HStore *store, char *key, unsigned int *vlen, uint32_t *flag)
{
    if (!key || !store) return NULL;

    if (key[0] == '@')
    {
        char *r = hs_list(store, key + 1);
        if (r) *vlen = strlen(r);
        *flag = 0;
        return r;
    }

    int info = 0;
    if (key[0] == '?')
    {
        info = 1;
        if (strlen(key) > 1 && key[1] == '?')
            info = 2;
        key += info;
    }
    int index = get_index(store, key);
    uint32_t ret_pos = 0;
    DataRecord *r = bc_get(store->bitcasks[index], key, &ret_pos, true);
    if (r == NULL)
        return NULL;

    char *res = NULL;
    if (info)
    {
        res = (char*)try_malloc(META_BUF_SIZE);

        uint16_t hash = 0;
        if (r->version > 0)
            hash = gen_hash(r->value, r->vsz);

        if (info == 2)
            *vlen = (size_t)safe_snprintf(res, META_BUF_SIZE, "%d %u %u %u %u %u %u", r->version,
                         hash, r->flag, r->vsz, r->tstamp, ret_pos & 0xff, ret_pos & 0xffffff00);
        else
            *vlen = (size_t)safe_snprintf(res, META_BUF_SIZE, "%d %u %u %u %u", r->version,
                         hash, r->flag, r->vsz, r->tstamp);

        *flag = 0;
    }
    else if (r->version > 0)
    {
        res = record_value(r);
        r->value = NULL;
        *vlen = r->vsz;
        *flag = r->flag;
    }
    free_record(&r);
    return res;
}

bool hs_set(HStore *store, char *key, char *value, unsigned int vlen, uint32_t flag, int ver)
{
    if (!store || !key || key[0] == '@') return false;
    if (store->before > 0) return false;

    int index = get_index(store, key);
    return bc_set(store->bitcasks[index], key, value, vlen, flag, ver);
}

bool hs_append(HStore *store, char *key, char *value, unsigned int vlen)
{
    if (!store || !key || key[0] == '@') return false;
    if (store->before > 0) return false;

    pthread_mutex_t *lock = get_mutex(store, key);
    pthread_mutex_lock(lock);

    int suc = false;
    unsigned int rlen = 0;
    uint32_t flag = (uint32_t)APPEND_FLAG;
    char *body = hs_get(store, key, &rlen, &flag);
    if (body != NULL && flag != APPEND_FLAG)
    {
        printf("try to append %s with flag=%x\n", key, flag);
        goto APPEND_END;
    }
    body = (char*)safe_realloc(body, rlen + vlen);
    memcpy(body + rlen, value, vlen); // safe
    suc = hs_set(store, key, body, rlen + vlen, flag, 0); // TODO: use timestamp

APPEND_END:
    if (body != NULL) free(body);
    pthread_mutex_unlock(lock);
    return suc;
}

int64_t hs_incr(HStore *store, char *key, int64_t value)
{
    if (!store || !key || key[0] == '@') return 0;
    if (store->before > 0) return 0;

    pthread_mutex_t *lock = get_mutex(store, key);
    pthread_mutex_lock(lock);

    int64_t result = 0;
    unsigned int rlen = 0;
    uint32_t flag = (uint32_t)INCR_FLAG;
    char buf[25];
    char *body = hs_get(store, key, &rlen, &flag);

    if (body != NULL)
    {
        if (flag != INCR_FLAG || rlen > 22)
        {
            printf("try to incr %s but flag=0x%x, len=%u\n", key, flag, rlen);
            goto INCR_END;
        }

        body = safe_realloc(body, rlen + 1);
        body[rlen] = 0;
        result = strtoll(body, NULL, 10);
        if (result == 0 && errno == EINVAL)
        {
            printf("incr %s failed: %s\n", key, buf);
            goto INCR_END;
        }
    }

    result += value;
    if (result < 0) result = 0;
    rlen = safe_snprintf(buf, 25, "%lld", (long long int) result);
    if (!hs_set(store, key, buf, rlen, INCR_FLAG, 0))   // use timestamp later
    {
        result = 0; // set failed
    }

INCR_END:
    pthread_mutex_unlock(lock);
    if (body != NULL) free(body);
    return result;
}

void *do_optimize(void *arg)
{
    pthread_detach(pthread_self());
    HStore *store = (HStore *) arg;
    time_t st = time(NULL);
    printf("start to optimize from 0x%x to 0x%x, limit %d\n",
            store->op_start, store->op_end - 1 , store->op_limit);
    store->op_laststat = 0;
    for (; store->op_start < store->op_end && store->op_laststat == 0; ++(store->op_start))
    {
        store->op_laststat = bc_optimize(store->bitcasks[store->op_start], store->op_limit);
    }
    store->op_start = store->op_end = 0;
    printf("optimization %s in %lld seconds\n",
           store->op_laststat >=0 ?"completed":"failed",  (long long)(time(NULL) - st));
    return NULL;
}

static bool tree2range(char *tree, int height, int *start, int *end)
{
    int count = 1 << (height * 4);
    *start = 0;
    *end = count;

    int len = strlen(tree);
    int shift = (height - (len - 1))* 4;
    if (len < 1 || tree[0] != '@' || shift < 0)
    {
        return false;
    }

    if (len > 1)
    {
        long n = 0;
        if (!safe_strtol(tree + 1, 16, &n) || n > count-1  || n < 0)
        {
            return false;
        }
        *start = n << shift;
        *end = (n + 1) << shift;
        if (*start < 0 || *start > count || *end < 0 || *end > count)
            return false;
    }
    return true;
}

int hs_optimize(HStore *store, long limit, char *tree)
{
    if (store->before > 0)
        return  -1;
    if (store->op_start < store->op_end)
        return  -2;

    int start, end;
    if (!tree2range(tree, store->height, &start, &end))
        return -3;

    pthread_t id;
    store->op_limit = limit;
    store->op_start = start;
    store->op_end = end;
    pthread_create(&id, NULL, do_optimize, store);

    return 0;
}

//>=0 running; == -1 ok; < -1 err
int hs_optimize_stat(HStore *store)
{
    if (store->op_start < store->op_end)
        return store->op_start;
    else
        return store->op_laststat - 1;
}

bool hs_delete(HStore *store, char *key)
{
    if (!key || !store) return false;
    if (store->before > 0) return false;

    int index = get_index(store, key);
    return bc_delete(store->bitcasks[index], key);
}

uint64_t hs_count(HStore *store, uint64_t *curr)
{
    uint64_t total = 0, curr_total = 0;
    int i;
    for (i = 0; i < store->count; i++)
    {
        uint32_t curr = 0;
        total += bc_count(store->bitcasks[i], &curr);
        curr_total += curr;
    }

    if (NULL != curr)  *curr = curr_total;
    return total;
}

void hs_stat(HStore *store, uint64_t *total, uint64_t *avail)
{
    uint64_t used = 0;
    *total = 0;
    int i;
    for (i = 0; i < store->count; i++)
    {
        bc_stat(store->bitcasks[i], &used);
        *total += used;
    }

    uint64_t total_space;
    mgr_stat(store->mgr, &total_space, avail);
}
