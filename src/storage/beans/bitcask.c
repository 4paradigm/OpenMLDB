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

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>
#include <time.h>
#include <inttypes.h>
#include <dirent.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "bitcask.h"
#include "htree.h"
#include "record.h"
#include "diskmgr.h"
#include "hint.h"
#include "const.h"


#define MAX_BUCKET_COUNT 256

extern struct settings settings;
const uint32_t WRITE_BUFFER_SIZE = 2 << 20; // 2M

const int SAVE_HTREE_LIMIT = 5;

const char DATA_FILE[] = "%s/%03d.data";
const char HINT_FILE[] = "%s/%03d.hint.qlz";
const char HTREE_FILE[] = "%s/%03d.htree";

struct bitcask_t
{
    uint32_t depth, pos;
    time_t before;
    Mgr    *mgr;
    HTree  *tree, *curr_tree;
    int    last_snapshot;
    int    curr;
    uint64_t bytes, curr_bytes;
    char   *write_buffer;
    time_t last_flush_time;
    uint32_t    wbuf_size, wbuf_start_pos, wbuf_curr_pos;
    pthread_mutex_t flush_lock, buffer_lock, write_lock;
    int    optimize_flag;
    char   *flush_buffer;
    uint32_t    fbuf_size, fbuf_start_pos;
    int     flushing_bucket;
    int64_t buckets[256];
};

static inline bool file_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0;
}

static inline char *gen_path(char *dst, int dst_size, const char *base, const char *fmt, int i)
{
    safe_snprintf(dst, dst_size , fmt,  base, i);
    return dst;
}

static inline char *new_path_real(char *dst, int dst_size, Mgr *mgr, const char *fmt, int i)
{
    char name[16];
    safe_snprintf(name, 16, fmt + 3, i);
    safe_snprintf(dst, dst_size, "%s/%s",  mgr_alloc(mgr, name), name);
    printf("mgr_alloc %s\n", dst);
    return dst;
}

static inline char *new_path(char *dst, int dst_size, Mgr *mgr, const char *fmt, int i)
{
    char *path = gen_path(dst, dst_size, mgr_base(mgr), fmt, i);
    mgr_unlink(path);
    return new_path_real(dst, dst_size, mgr, fmt, i);
}

int dump_buckets(Bitcask *bc);
static inline char *new_data(char *dst, int dst_size, Bitcask *bc, const char *fmt, int i)
{
    char *path = gen_path(dst, dst_size, mgr_base(bc->mgr), fmt, i);
    if (bc->buckets[i] >= 0)
        return path;

    struct stat st;
    if (stat(path, &st) == 0)
    {
        printf("Bug: %s should not exist, exit!\n", path);
        exit(-1);
    }
    bc->buckets[i] = 0;
    dump_buckets(bc);
    return new_path_real(dst, dst_size, bc->mgr, fmt, i);
}


#define MAX_BUCKETS_FILE_SIZE (256 * 32)
int load_buckets(const char *base, int64_t *buckets, int *last)
{
    char path[MAX_PATH_LEN];
    safe_snprintf(path, MAX_PATH_LEN, "%s/buckets.txt", base);
    char buf[MAX_BUCKETS_FILE_SIZE];
    struct stat  st;
    if (0 != stat(path, &st))
    {
        printf("file %s not exist, no data!\n", path);
        return 0;
    }
    printf("loading buckets %s\n", path);
    if (st.st_size > MAX_BUCKETS_FILE_SIZE)
    {
        printf("file %s too large\n", path);
        return -1;
    }
    FILE *f = fopen(path,"r");
    if (f == NULL)
    {
        printf("fail to open file %s, err:%s\n", path, strerror(errno));
        return -1;
    }
    int n = fread(buf, 1, st.st_size, f);
    if (n < st.st_size)
    {
        printf("fail to open file %s, err:%s\n", path, strerror(errno));
        return -1;
    }

    buf[n] = 0;
    char *p = buf;
    char *endptr;
    while(p-buf < n)
    {
        long bucket = strtol(p, &endptr, 10);
        if (p == endptr)
            continue;
        if (bucket < 0 || bucket > 255 || bucket <= *last)
        {
            printf("bad bucket: %ld\n", bucket);
            return -1;
        }
        *last = bucket;

        p = endptr + 1;
        long long size = strtoll(p, &endptr, 10);
        if (p == endptr)
        {
            printf("bad file %s\n", path);
            return -1;
        }
        printf("buckets[%ld] = %lld\n", bucket, size);
        buckets[bucket] = size;
        p = endptr + 1;
    }
    fclose(f);
    return 1;
}

int dump_buckets(Bitcask *bc)
{
    char buf[MAX_BUCKETS_FILE_SIZE];

    char *p = buf;
    int i;
    for (i = 0; i < 256; i++)
    {
        if (bc->buckets[i] >= 0)
        {
            int n = safe_snprintf(p, buf + MAX_BUCKETS_FILE_SIZE - p ,"%d %"PRIi64"\n", i, bc->buckets[i]);
            p += n;
        }
    }

    *p = 0;
    char path[MAX_PATH_LEN];
    safe_snprintf(path, MAX_PATH_LEN, "%s/buckets.txt", mgr_base(bc->mgr));
    if (p == buf)
    {
        printf("no data, delete %s\n", path);
        unlink(path);
        return -1;
    }
    printf("dumping buckets");

    FILE *f = fopen(path,"w");
    if (f == NULL)
    {
        printf("fail to open %s\n", path);
        return -1;
    }
    int n = fwrite(buf, 1, p - buf, f);
    if (n < p - buf)
    {
        printf("fail to write %s\n", path);
        fclose(f);
        return -1;
    }
    fclose(f);
    return 0;
}

int get_bucket_by_name(char *dir, char *name, long *bucket)
{
    if (name[0] == '.' || strcmp("buckets.txt",name) == 0)
        return -1;

    if (strlen(name) + strlen(dir) > MAX_PATH_LEN)
    {
        printf("find long name %s/%s\n", dir, name);
        return -1;
    }

    const char *types[] = {DATA_FILE, HINT_FILE, HTREE_FILE};
    char *endptr;
    errno  = 0;
    *bucket  = strtol(name, &endptr, 10);
    if (errno == ERANGE || (endptr - name) != 3 || *bucket > 255 || *bucket < 0)
    {
        struct stat sb;
        char tmp[MAX_PATH_LEN];
        snprintf(tmp, MAX_PATH_LEN, "%s/%s", dir, name);
        if (0 != stat(tmp, &sb) || ((sb.st_mode & S_IFMT) != S_IFDIR))
            printf("find unexpect file %s/%s\n", dir, name);
        return -1;
    }
    char *suffix = name + 3;
    if (0 == strcmp(name + strlen(name) - 3, "tmp"))
    {
        printf("find tmp file %s/%s\n", dir, name);
        return -1;
    }
    int i;
    for (i = 0; i < 3; i++)
    {
        if (strcmp(types[i] + 7, suffix) == 0)
        {
            return i;
        }
    }
    printf("find unexpect file %s/%s\n", dir, name);
    return -1;
}

int check_buckets(Mgr *mgr, int64_t *sizes, int locations[][3])
{
    char **disks = mgr->disks;
    struct stat sb;
    char path[MAX_PATH_LEN], sym[MAX_PATH_LEN], real[MAX_PATH_LEN];

    int i;
    for (i = 0; i < mgr->ndisks; i++)
    {
        DIR* dp = opendir(disks[i]);
        if (dp == NULL)
        {
            printf("opendir failed: %s!\n", disks[i]);
            return -1;
        }

        struct dirent *de;
        while ((de = readdir(dp)) != NULL)
        {
            char *name = de->d_name;
            long bucket = -1;
            int  type = get_bucket_by_name(disks[i], name, &bucket);
            if (type < 0)
                continue;

            safe_snprintf(path, MAX_PATH_LEN, "%s/%s", disks[i], name);
            lstat(path, &sb);
            if (i == 0)
            {
                if ((sb.st_mode & S_IFMT) == S_IFLNK)
                {
                    long bucket_real = -1;
                    int  type_real = 0;
                    if (mgr_readlink(path, real, MAX_PATH_LEN) <= 0
                            || (type_real = get_bucket_by_name(disks[i], simple_basename(real), &bucket_real)) < 0
                            || type_real != type || bucket_real != bucket)
                    {
                        printf("find bad symlink %s->%s, type = %d, bucket = %ld\n", path, real, type_real, bucket_real);
                        return -1;
                    }
                    if (stat(real, &sb) != 0)
                    {
                        locations[bucket][type] = -2;
                        printf("find empty symlink %s\n", real);
                    }
                    else
                    {
                        locations[bucket][type] = -2;
                        char real2[MAX_PATH_LEN];
                        int j;
                        for (j = 1; j < mgr->ndisks; j++)
                        {
                            safe_snprintf(real2, MAX_PATH_LEN, "%s/%s",  mgr->disks[j], name);
                            struct stat sb2;
                            if (stat(real2, &sb2) == 0 && sb2.st_dev == sb.st_dev && sb2.st_ino == sb.st_ino)
                            {
                                locations[bucket][type] = j;
                                break;
                            }
                        }
                    }
                }
                else
                {
                    locations[bucket][type] = 0;
                }
            }
            else
            {
                if ((sb.st_mode & S_IFMT) != S_IFREG)
                {
                    printf("find non-regule file on non-zero disk %s/%s\n", disks[i], name);
                    return -1;
                }
                int old_loc = locations[bucket][type];
                if (old_loc == -1)
                {
                    if (settings.autolink)
                    {
                        safe_snprintf(sym, MAX_PATH_LEN, "%s/%s", disks[0], name);
                        if (symlink(path, sym) != 0)
                        {
                            printf("symlink failed %s -> %s, err: %s !\n", sym, path, strerror(errno));
                            return -1;
                        }
                        else
                        {
                            printf("auto link for %s\n", path);
                            locations[bucket][type] = i;
                        }
                    }
                    else
                    {
                        printf("file not linked %s!\n", path);
                        return -1;
                    }
                }
                else if (old_loc != i)
                {
                    printf("find dup files %s in both %s and %s \n",  name, disks[i], disks[old_loc]);
                    return -1;
                }
            }
            if (type == 0)
            {
                if (stat(path, &sb) == 0)
                {
                    sizes[bucket] =  sb.st_size;
                    if (sb.st_size % 256 != 0)
                    {
                        printf("size of %s is 0x%llx, not aligned\n", path, (long long)sb.st_size);
                    }
                }
                else
                {
                    sizes[bucket] =  0;
                    //printf("find empty link %s, unlink!", path);
                    //unlink(path);
                }
            }
            //printf("bucket = %ld, type = %d, size = %"PRIu64"", bucket, type, sizes[bucket]);
        }//end while readdir
        (void) closedir(dp);
    }
    return 0;
}


Bitcask* bc_open(const char *path, int depth, int pos, time_t before)
{
    if (path == NULL || depth > 4) return NULL;
    if (0 != access(path, F_OK) && 0 != mkdir(path, 0750))
    {
        printf("mkdir %s failed\n", path);
        return NULL;
    }
    const char *t[] = {path};
    Mgr *mgr = mgr_create(t, 1);
    if (mgr == NULL) return NULL;

    Bitcask* bc = bc_open2(mgr, depth, pos, before);
    if (bc != NULL) bc_scan(bc);
    return bc;
}

static void print_buckets(int64_t *buckets)  // NOLINT
{
    int i;
    printf("\n");
    for (i = 0; i < 256; i++)
    {
        if (buckets[i] >= 0)
        {
            printf("%d : %"PRIu64"\n", i, buckets[i]);
        }
    }
    printf("\n");
}

static void init_buckets(Bitcask *bc)
{
    memset(bc->buckets, -1, sizeof(int64_t)*256);

    int locations[256][3];
    memset(locations, -1, sizeof(int)*256*3);
    if (check_buckets(bc->mgr, bc->buckets, locations) != 0 )
    {
        printf("bitcask 0x%x check failed, exit!\n", bc->pos);
        exit(-1);
    }
    //print_buckets(bc->buckets);
    if (settings.check_file_size)
    {
        int64_t buckets[256];
        memset(buckets, -1, sizeof(int64_t)*256);
        int last  = -1;
        int ret = load_buckets(mgr_base(bc->mgr), buckets, &last);
        if (ret < 0)
        {
            printf("load_buckets fail, bc %0x, exit\n", bc->pos);
            exit(1);
        }
        else if (ret == 0 )
        {
            if (bc->buckets[0] >= 0)
            {
                printf("bucket.txt not exist , bc %0x\n", bc->pos);
            }
        }
        else
        {
            int i;
            for (i = 255; i >= 0; i--)
            {
                if (buckets[i] != bc->buckets[i])
                {
                    if (i > last && bc->buckets[i] >= 0) //buckets[last] = -1
                    {
                        printf("last file not in buckets.txt (bc %0x, bucket %d)\n", bc->pos, i);
                    }
                    else if (i == last && buckets[i] >= 0 && bc->buckets[i] >= 0)
                    {
                        printf("last file size not match (bc %0x, bucket %d)\n", bc->pos, i);
                    }
                    else
                    {
                        printf("bucket size not match (bc %0x, bucket %d), exit\n", bc->pos, i);
                        exit(1);
                    }
                }
            }
        }
    }

    int i;
    char path[MAX_PATH_LEN];
    for (i=0; i<256; i++)
    {
        if (-1 == locations[i][0])
        {
            if (locations[i][1] != -1)
                printf(" unused file: %s\n",gen_path(path, MAX_PATH_LEN, mgr_base(bc->mgr), HINT_FILE, i));

            if (locations[i][2] != -1)
                printf(" unused file: %s\n",gen_path(path, MAX_PATH_LEN, mgr_base(bc->mgr), HTREE_FILE, i));
        }
    }
    //print_buckets(bc->buckets);
}

Bitcask* bc_open2(Mgr *mgr, int depth, int pos, time_t before)
{
    Bitcask* bc = (Bitcask*)beans_safe_malloc(sizeof(Bitcask));
    /*if (bc == NULL) return NULL;*/

    memset(bc, 0, sizeof(Bitcask));
    bc->mgr = mgr;
    bc->depth = depth;
    bc->pos = pos;
    bc->before = before;
    bc->bytes = 0;
    bc->curr_bytes = 0;
    bc->tree = NULL;
    bc->last_snapshot = -1;
    bc->curr_tree = ht_new(depth, pos, true);
    bc->wbuf_size = 1024 * 4;
    bc->write_buffer = (char*)beans_safe_malloc(bc->wbuf_size);
    bc->last_flush_time = time(NULL);
    bc->flush_buffer = NULL;
    bc->fbuf_start_pos = 0;
    bc->fbuf_size = 0;
    bc->flushing_bucket = -1;
    pthread_mutex_init(&bc->buffer_lock, NULL);
    pthread_mutex_init(&bc->write_lock, NULL);
    pthread_mutex_init(&bc->flush_lock, NULL);
    init_buckets(bc);
    return bc;
}

static void skip_empty_file(Bitcask *bc)
{
    int i, last=0;
    char opath[MAX_PATH_LEN], npath[MAX_PATH_LEN];
    const char *base = mgr_base(bc->mgr);
    for (i = 0; i < MAX_BUCKET_COUNT; i++)
    {
        int64_t size = bc->buckets[i];
        gen_path(opath, MAX_PATH_LEN, base, DATA_FILE, i);
        if (size > 0)
        {
            if (i != last)
            {
                mgr_rename(opath, gen_path(npath, MAX_PATH_LEN, base, DATA_FILE, last));
                if (file_exists(gen_path(opath, MAX_PATH_LEN, base, HINT_FILE, i)))
                {
                    mgr_rename(opath, gen_path(npath, MAX_PATH_LEN, base, HINT_FILE, last));
                }
                mgr_unlink(gen_path(opath, MAX_PATH_LEN, base, HTREE_FILE, i));
                bc->buckets[last] = bc->buckets[i];
                bc->buckets[i] = -1;
            }
            ++last;
        }
        else if (size == 0)
        {
            struct stat sb;
            if (lstat(opath, &sb) != 0)
            {
                printf("%s(either link or file) should exist on disk0 at least, exit\n", opath);
                exit(1);
            }
            if (stat(opath, &sb) == 0 && sb.st_size > 0)
            {
                printf("Bug: size of %s should be 0, but is %lld!\n", opath, (long long)sb.st_size);
                exit(1);
            }

            //cases:
            //  abnormal empty link/file
            //      exit() or killed, no chance to flush
            //      rotate and no writing
            //      gc failed
            //  normal empty file:
            //      gc result
            printf("rm empty bucket %s\n", opath);
            bc->buckets[i] = -1;
            mgr_unlink(opath);
            mgr_unlink(gen_path(opath, MAX_PATH_LEN, base, HINT_FILE, i));
            mgr_unlink(gen_path(opath, MAX_PATH_LEN, base, HTREE_FILE, i));
        }
    }
}

void bc_scan(Bitcask *bc)
{
    char datapath[MAX_PATH_LEN], hintpath[MAX_PATH_LEN];
    int i = 0;
    struct stat st, hst;

    skip_empty_file(bc);
    dump_buckets(bc);

    const char *base = mgr_base(bc->mgr);
    // load snapshot of htree
    for (i = MAX_BUCKET_COUNT - 1; i >= 0; --i)
    {
        if (stat(gen_path(datapath, MAX_PATH_LEN, base, HTREE_FILE, i), &st) == 0
                && stat(gen_path(hintpath, MAX_PATH_LEN, base, HINT_FILE, i), &hst) == 0
                && st.st_mtime >= hst.st_mtime
                && (bc->before == 0 || st.st_mtime < bc->before))
        {
            bc->tree = ht_open(bc->depth, bc->pos, datapath);
            if (bc->tree != NULL)
            {
                bc->last_snapshot = i;
                break;
            }
            else
            {
                printf("open HTree from %s failed\n", datapath);
                mgr_unlink(datapath);
            }
        }
    }
    if (bc->tree == NULL)
    {
        bc->tree = ht_new(bc->depth, bc->pos, false);
    }

    for (i = 0; i < MAX_BUCKET_COUNT; i++)
    {
        if (stat(gen_path(datapath, MAX_PATH_LEN, base, DATA_FILE, i), &st) != 0)
        {
            break;
        }
        bc->bytes += st.st_size;
        if (i <= bc->last_snapshot) continue;

        gen_path(hintpath, MAX_PATH_LEN, base, HINT_FILE, i);
        if (bc->before == 0)
        {
            if (0 == stat(hintpath, &st))
            {
                scanHintFile(bc->tree, i, hintpath, NULL);
            }
            else
            {
                scanDataFile(bc->tree, i, datapath,
                             new_path(hintpath, MAX_PATH_LEN, bc->mgr, HINT_FILE, i));
            }
        }
        else
        {
            if (0 == stat(hintpath, &st) &&
                    (st.st_mtime < bc->before || (0 == stat(datapath, &st) && st.st_mtime < bc->before)))
            {
                scanHintFile(bc->tree, i, hintpath, NULL);
            }
            else
            {
                scanDataFileBefore(bc->tree, i, datapath, bc->before);
            }
        }
    }

    if (i - bc->last_snapshot > SAVE_HTREE_LIMIT)
    {
        if (ht_save(bc->tree, new_path(datapath, MAX_PATH_LEN, bc->mgr, HTREE_FILE, i-1)) == 0)
        {
            char htreepath_tmp[MAX_PATH_LEN];
            mgr_unlink(gen_path(htreepath_tmp, MAX_PATH_LEN, base, HTREE_FILE, bc->last_snapshot));
            bc->last_snapshot = i-1;
        }
        else
        {
            printf("save HTree to %s failed\n", datapath);
        }
    }

    bc->curr = i;
    if (i > 0)
    {
        printf("bitcask %x loaded, curr = %d\n", bc->pos , i);
    }
}

/*
 * bc_close() is not thread safe, should stop other threads before call it.
 * */
void bc_close(Bitcask *bc)
{
    char datapath[MAX_PATH_LEN], hintpath[MAX_PATH_LEN];

    if (bc->optimize_flag > 0)
    {
        bc->optimize_flag = 2;
        while (bc->optimize_flag > 0)
        {
            sleep(1);
        }
    }

    pthread_mutex_lock(&bc->write_lock);

    bc_flush(bc, 0, 0);
    struct stat sb;
    if (stat(gen_path(datapath, MAX_PATH_LEN, mgr_base(bc->mgr), DATA_FILE, bc->curr), &sb) == 0)
    {
        bc->buckets[bc->curr] = sb.st_size;
        dump_buckets(bc);
    }

    if (NULL != bc->curr_tree)
    {
        if (bc->curr_bytes > 0)
        {
            build_hint(bc->curr_tree, new_path(hintpath, MAX_PATH_LEN, bc->mgr, HINT_FILE, bc->curr));
        }
        else
        {
            ht_destroy(bc->curr_tree);
        }
        bc->curr_tree = NULL;
    }

    if (bc->curr_bytes == 0) --(bc->curr);
    if (bc->curr - bc->last_snapshot >= SAVE_HTREE_LIMIT)
    {
        if (ht_save(bc->tree, new_path(datapath, MAX_PATH_LEN, bc->mgr, HTREE_FILE, bc->curr)) == 0)
        {
            mgr_unlink(gen_path(datapath, MAX_PATH_LEN, mgr_base(bc->mgr), HTREE_FILE, bc->last_snapshot));
        }
        else
        {
            printf("save HTree to %s failed\n", datapath);
        }
    }
    ht_destroy(bc->tree);

    mgr_destroy(bc->mgr);
    free(bc->write_buffer);
    free(bc);
}

uint64_t data_file_size(Bitcask *bc, int bucket)
{
    struct stat st;
    char path[MAX_PATH_LEN];
    gen_path(path, MAX_PATH_LEN, mgr_base(bc->mgr), DATA_FILE, bucket);
    if (stat(path, &st) != 0) return 0;
    return st.st_size;
}


// update pos in HTree
struct update_args
{
    HTree *tree;
    uint32_t index;
};

static void update_item_pos(Item *it, void *_args)
{
    struct update_args *args = (struct update_args*)_args;
    HTree *tree = (HTree*) args->tree;
    Item *p = ht_get(tree, it->key);
    if (p)
    {
        if (it->pos == p->pos)
        {
            uint32_t npos = (it->pos & 0xffffff00) | args->index;
            ht_add(tree, p->key, npos, p->hash, p->ver);
        }
        free(p);
    }
}

int bc_optimize(Bitcask *bc, int limit)
{
    int i, total, last = -1;
    bc->optimize_flag = 1;
    const char *base = mgr_base(bc->mgr);
    char htreepath_tmp[MAX_PATH_LEN];
    // remove htree
    for (i = 0; i < bc->curr; ++i)
    {
        mgr_unlink(gen_path(htreepath_tmp, MAX_PATH_LEN, base, HTREE_FILE, i));
    }
    bc->last_snapshot = -1;

    time_t limit_time = 0;
    if (limit > 3600 * 24 * 365 * 10)   // more than 10 years
    {
        limit_time = limit; // absolute time
    }
    else
    {
        limit_time = time(NULL) - limit; // relative time
    }

    struct stat st;
    bool skipped = false;
    for (i = 0; i < bc->curr && bc->optimize_flag == 1; ++i)
    {
        char datapath[MAX_PATH_LEN], hintpath[MAX_PATH_LEN];
        gen_path(datapath, MAX_PATH_LEN, base, DATA_FILE, i);
        gen_path(hintpath, MAX_PATH_LEN, base, HINT_FILE, i);
        if (bc->buckets[i] < 0)
        {
            if (stat(datapath, &st) == 0)
            {
                printf("data file: %s should not exist\n", datapath);
            }
            continue; // skip empty file
        }
        else
        {
            if (stat(datapath, &st) != 0)
            {
                printf("data file: %s lost\n", datapath);
                return -1;
            }
        }
        // skip recent modified file
        if (st.st_mtime > limit_time)
        {
            skipped = true;
            printf("optimize skip %s\n", datapath);

            ++last;
            if (last != i)   // rotate data file
            {
                // update HTree to use new index
                if (stat(hintpath, &st) != 0)
                {
                    printf("no hint file: %s, skip it\n", hintpath);
                    last = i;
                    continue;
                }

                char npath[MAX_PATH_LEN];
                gen_path(npath, MAX_PATH_LEN, base, DATA_FILE, last);
                if (symlink(datapath, npath) != 0)
                {
                    printf("symlink failed: %s -> %s, err:%s\n", datapath, npath, strerror(errno));
                    bc->optimize_flag = 0;
                    return -1;
                }

                HTree *tree = ht_new(bc->depth, bc->pos, true);
                scanHintFile(tree, i, hintpath, NULL);
                struct update_args args;
                args.tree = bc->tree;
                args.index = last;
                ht_visit(tree, update_item_pos, &args);
                ht_destroy(tree);

                unlink(npath);
                mgr_rename(datapath, npath);
                mgr_rename(hintpath, gen_path(npath, MAX_PATH_LEN, base, HINT_FILE, last));

                bc->buckets[last] = bc->buckets[i];
                bc->buckets[i] = -1;
                dump_buckets(bc);
            }
            continue;
        }

        int deleted = count_deleted_record(bc->tree, i, hintpath, &total, skipped);
        uint64_t curr_size = data_file_size(bc, i) * (total - deleted/2) / (total+1); // guess
        uint64_t last_size = last >= 0 ? data_file_size(bc, last) : -1;

        uint32_t bytes_deleted= 0;
        if (last == -1 || last_size + curr_size > settings.max_bucket_size)
        {
            ++last;
        }
        int last0 = last;
        while (last <= last0 + 1)
        {
            char ldpath[MAX_PATH_LEN], lhpath[MAX_PATH_LEN], lhpath_real[MAX_PATH_LEN];
            gen_path(ldpath, MAX_PATH_LEN, mgr_base(bc->mgr), DATA_FILE, last);
            struct stat sb;
            if ((bc->buckets[last]>= 0) !=  (lstat(ldpath,&sb)==0))
            {
                printf("buckets mismatch!");
                bc->optimize_flag = 0;
                return -1;
            }

            gen_path(lhpath, MAX_PATH_LEN, mgr_base(bc->mgr), HINT_FILE, last);
            if (mgr_getrealpath(lhpath, lhpath_real, MAX_PATH_LEN) != 0)
            {
                new_path(lhpath_real, MAX_PATH_LEN, bc->mgr, HINT_FILE, last);
            }

            int ret = optimizeDataFile(bc->tree, bc->mgr, i, datapath, hintpath, last, ldpath, lhpath_real,
                    settings.max_bucket_size, skipped, (last == i) || (last != i && bc->buckets[last] < 0), &bytes_deleted);

            if (ret == 0)
            {
                struct stat sb;
                if (stat(ldpath, &sb) == 0)
                {
                    bc->buckets[i] = -1;
                    bc->buckets[last]  = sb.st_size;
                    dump_buckets(bc);
                }
                else{
                    printf("last %s not exist after gc:\n", ldpath);
                    bc->optimize_flag = 0;
                    return -1;
                }
                break;
            }
            else if (ret < 0 )
            {
                bc->optimize_flag = 0;
                return -1;
            }
            else
            {
                if (last < i)
                {
                    printf("fail to optimize %s into %d, try next\n", datapath, last);
                    last ++;
                }
                else
                {
                    printf("Bug: fail to optimize %s into %d self, return\n", datapath, last);
                    bc->optimize_flag = 0;
                    return -1;
                }
            }
        }

        pthread_mutex_lock(&bc->buffer_lock);
        bc->bytes -= bytes_deleted;
        pthread_mutex_unlock(&bc->buffer_lock);
    }

    // update pos of items in curr_tree
    pthread_mutex_lock(&bc->write_lock);
    pthread_mutex_lock(&bc->flush_lock);
    if (i == bc->curr && ++last < bc->curr)
    {
        char opath[MAX_PATH_LEN], npath[MAX_PATH_LEN];
        gen_path(opath, MAX_PATH_LEN, base, DATA_FILE, bc->curr);

        if (bc->buckets[bc->curr] > 0)
        {
            gen_path(npath, MAX_PATH_LEN, base, DATA_FILE, last);
            if (symlink(opath, npath) != 0)
                printf("symlink failed: %s -> %s, err:%s\n", opath, npath, strerror(errno));
        }

        struct update_args args;
        args.tree = bc->tree;
        args.index = last;
        ht_visit(bc->curr_tree, update_item_pos, &args);

        if (bc->buckets[bc->curr] > 0)
        {
            unlink(npath);
            mgr_rename(opath, npath);
        }

        bc->buckets[last] = bc->buckets[i];
        bc->buckets[i] = -1;
        dump_buckets(bc);

        bc->curr = last;
    }
    pthread_mutex_unlock(&bc->flush_lock);
    pthread_mutex_unlock(&bc->write_lock);
    if (last > 0)
        printf("bitcask %x optimization done, curr = %d, last = %d\n", bc->pos, bc->curr, last);
    bc->optimize_flag = 0;
    return 0;
}

DataRecord* bc_get(Bitcask *bc, const char *key, uint32_t *ret_pos, bool return_deleted)
{
    if (!check_key(key, strlen(key)))
        return NULL;


    int maybe_tmp = 0;
    char buf[512];
    Item *item = ht_get_maybe_tmp(bc->tree, key, &maybe_tmp, buf);
    if (NULL == item) return NULL;

    *ret_pos = item->pos;

    if (!return_deleted && item->ver < 0)
        return NULL;

    uint32_t bucket = item->pos & 0xff;
    uint32_t pos = item->pos & 0xffffff00;

    if (bucket > (uint32_t)(bc->curr))
    {
        printf("Bug: invalid bucket %d > %d, bitcask %x, key = %s\n", bucket, bc->curr, bc->pos, key);
        ht_remove(bc->tree, key);
        return NULL;
    }

    DataRecord *r = NULL;
    if (bucket == (uint32_t)(bc->curr) || bucket == (uint32_t)(bc->flushing_bucket))
    {
        pthread_mutex_lock(&bc->buffer_lock);
        if (bucket == (uint32_t)(bc->curr) && pos >= bc->wbuf_start_pos)
        {
            uint32_t p = pos - bc->wbuf_start_pos;
            r = decode_record(bc->write_buffer + p, bc->wbuf_curr_pos - p, true, "wbuf", pos, key, true, NULL);
        }
        else if (bucket == (uint32_t)(bc->flushing_bucket) && pos >= bc->fbuf_start_pos)
        {
            if (bc->flush_buffer == NULL)
            {
                printf("Bug: flush_buf is NULL");
                pthread_mutex_unlock(&bc->buffer_lock);
                return NULL;
            }
            uint32_t p = pos - bc->fbuf_start_pos;
            r = decode_record(bc->flush_buffer + p, bc->fbuf_size - p, true, "fbuf", pos, key, true, NULL);
        }
        pthread_mutex_unlock(&bc->buffer_lock);

        if (r != NULL)
        {
            r->version = item->ver;
            return r;
        }
    }

    char datapath[MAX_PATH_LEN];
    gen_path(datapath, MAX_PATH_LEN, mgr_base(bc->mgr), DATA_FILE, bucket);
    if (maybe_tmp)
    {
        char tmp_path[MAX_PATH_LEN];
        safe_snprintf(tmp_path,  MAX_PATH_LEN, "%s.tmp", datapath);
        int tmp_fd = open(tmp_path, O_RDONLY);
        if (-1 != tmp_fd)
        {
            printf("success to open TMP file %s (to get %s)\n", tmp_path, key);
            r = fast_read_record(tmp_fd, pos, true, tmp_path, key);
            close(tmp_fd);
            if (NULL == r || strcmp(key, r->key) != 0)
            {
                goto READ_FAIL;
            }
            else
            {
                r->version = item->ver;
                return r;
            }
        }
        else
        {
            printf("fail to open TMP file %s (to get %s), will try to read the non-tmp\n", tmp_path, key);
        }
    }

    int fd = -1;
RETRY_READ:
    fd = open(datapath, O_RDONLY);
    if (-1 == fd)
    {
        if (bc->buckets[bucket] > 0)
            printf("fail to open %s, which should exist (to get key: %s), err:%s\n", datapath, key, strerror(errno));
        else
           printf("Bug: try read non-exist file %s (to get key %s)\n", datapath, key);
    }
    else
    {
        r = fast_read_record(fd, pos, true, datapath, key);
        close(fd);
    }

    //get old pos before updating, but read file after updating, may happen if file is small
    if(!maybe_tmp && (NULL == r || strcmp(key, r->key) != 0))
    {
        item = ht_get_withbuf(bc->tree, key, strlen(key), buf, true);
        if (NULL != item)
        {
            int new_pos = item->pos & 0xffffff00;
            if (new_pos != pos)
            {
                pos = new_pos;
                printf("get new pos, retry read %s (key %s)\n", datapath, key);
                goto RETRY_READ;
            }
            else
            {
                printf("get same pos = %d, path = %s, key = %s, \n", item->pos, datapath, key);
            }
        }
        else
        {
            printf("Bug: retry %s key = %s, get NULL\n", datapath, key);
        }
    }

READ_FAIL:
    if (NULL == r)
    {
        printf("Bug: get %s failed in %s @ %u\n", key, datapath, pos);
    }
    else if (strcmp(key, r->key) != 0)
    {
        printf("Bug: record %s is not expected %s in %s @ %u\n", r->key, key, datapath, pos);
        free_record(&r);
    }

    if (r != NULL)
        r->version = item->ver;
    else
        ht_remove(bc->tree, key);
    return r;
}

struct build_thread_args
{
    HTree *tree;
    char *path;
};

void *build_thread(void *param)
{
    pthread_detach(pthread_self());
    struct build_thread_args *args = (struct build_thread_args*) param;
    build_hint(args->tree, args->path);
    free(args->path);
    free(param);
    return NULL;
}

void bc_rotate(Bitcask *bc)
{
    // build in new thread
    char datapath[MAX_PATH_LEN], hintpath[MAX_PATH_LEN];
    new_path(hintpath, MAX_PATH_LEN, bc->mgr, HINT_FILE, bc->curr);
    struct build_thread_args *args = (struct build_thread_args*)beans_safe_malloc(
                                         sizeof(struct build_thread_args));
    args->tree = bc->curr_tree;
    args->path = strdup(hintpath);
    pthread_t build_ptid;
    pthread_create(&build_ptid, NULL, build_thread, args);

    struct stat sb;
    if (stat(gen_path(datapath, MAX_PATH_LEN, mgr_base(bc->mgr), DATA_FILE, bc->curr), &sb) == 0)
    {
        bc->buckets[bc->curr] = sb.st_size;
        dump_buckets(bc);
    }
    // next bucket
    bc->curr++;
    bc->curr_tree = ht_new(bc->depth, bc->pos, true);
    bc->wbuf_start_pos = 0;
    bc->curr_bytes = 0;
}

void bc_flush(Bitcask *bc, unsigned int limit, int flush_period)
{
    if (bc->curr >= MAX_BUCKET_COUNT)
    {
        printf("reach max bucket count");
        exit(1);
    }

    pthread_mutex_lock(&bc->flush_lock);
    pthread_mutex_lock(&bc->buffer_lock);

    time_t now = time(NULL);
    if (bc->wbuf_curr_pos > limit * 1024 ||
            (now > bc->last_flush_time + flush_period && bc->wbuf_curr_pos > 0))
    {
        bc->flushing_bucket = bc->curr;
        uint32_t size = bc->wbuf_curr_pos;
        bc->flush_buffer = (char*)beans_safe_malloc(size);
        memcpy(bc->flush_buffer, bc->write_buffer, size); // safe
        bc->fbuf_size = size;

        uint32_t last_pos = bc->wbuf_start_pos;
        if (bc->wbuf_size < WRITE_BUFFER_SIZE)
        {
            bc->wbuf_size *= 2;
            free(bc->write_buffer);
            bc->write_buffer = (char*)beans_safe_malloc(bc->wbuf_size);
        }
        else if (bc->wbuf_size > WRITE_BUFFER_SIZE * 2)
        {
            bc->wbuf_size = WRITE_BUFFER_SIZE;
            free(bc->write_buffer);
            bc->write_buffer = (char*)beans_safe_malloc(bc->wbuf_size);
        }

        bc->bytes += size;
        bc->curr_bytes += size;
        bc->fbuf_start_pos = bc->wbuf_start_pos;
        bc->wbuf_curr_pos -= size;
        bc->wbuf_start_pos += size;

        if (bc->wbuf_start_pos + bc->wbuf_size > settings.max_bucket_size)
        {
            printf("bitcask 0x%x bc_rotate after buffer write : curr %d -> %d, wbuf_size = %d, limit = %d, file size= %u, last_flush =  %d\n",
                    bc->pos, bc->curr, bc->curr+1, bc->wbuf_size, limit, bc->wbuf_start_pos, size);
            bc_rotate(bc);
        }
        pthread_mutex_unlock(&bc->buffer_lock);

        char buf[MAX_PATH_LEN];
        new_data(buf, MAX_PATH_LEN, bc, DATA_FILE, bc->flushing_bucket);

        FILE *f = fopen(buf, "ab");
        if (f == NULL)
        {
            printf("open file %s for flushing failed. exit!\n", buf);
            exit(1);
        }
        // check file size
        uint64_t file_size = ftello(f);
        if (last_pos > 0 && last_pos != file_size)
        {
            printf("last pos not match: %"PRIu64" != %u in %s. exit!\n", file_size, last_pos, buf);
            exit(1);
        }

        size_t n = fwrite(bc->flush_buffer, 1, size, f);
        if (n < size)
        {
            printf("write failed: return %zu. exit!\n", n);
            exit(1);
        }
        bc->buckets[bc->flushing_bucket] = file_size + size;
        if (file_size == 0 || now - bc->last_flush_time > 3600)
        {
            dump_buckets(bc);
        }
        fclose(f);
        bc->last_flush_time = now;

        pthread_mutex_lock(&bc->buffer_lock);
        bc->flushing_bucket = -1;
        free(bc->flush_buffer);
        bc->flush_buffer = NULL;
    }

    pthread_mutex_unlock(&bc->buffer_lock);
    pthread_mutex_unlock(&bc->flush_lock);
}

bool bc_set(Bitcask *bc, const char *key, char *value, size_t vlen, int flag, int version)
{
    if ((version < 0 && vlen > 0) || vlen > MAX_VALUE_LEN || !check_key(key, strlen(key)))
    {
        printf("invalid set cmd, key %s, version %d, vlen %ld\n", key, version, vlen);
        return false;
    }
    else
    {
        if (vlen > MAX_VALUE_LEN_WARN)
            printf("set large value for key %s, version %d, vlen %ld\n", key, version, vlen);
    }

    bool suc = false;
    pthread_mutex_lock(&bc->write_lock);

    int oldv = 0, ver = version;
    Item *it = ht_get(bc->tree, key);
    if (it != NULL)
    {
        oldv = it->ver;
    }

    if (version == 0 && oldv > 0)  // replace
    {
        ver = oldv + 1;
    }
    else if (version == 0 && oldv <= 0)    // add
    {
        ver = -oldv + 1;
    }
    else if (version < 0 && oldv <= 0)     // delete, not exist
    {
        goto SET_FAIL;
    }
    else if (version == -1)     // delete
    {
        ver = - abs(oldv) - 1;
    }
    else if (abs(version) <= abs(oldv))     // sync
    {
        goto SET_FAIL;
    }
    else     // sync
    {
        ver = version;
    }

    uint16_t hash = 0;
    if (ver > 0)
        hash = gen_hash(value, vlen);

    if (NULL != it && hash == it->hash)
    {
        uint32_t ret_pos = 0;
        DataRecord *r = bc_get(bc, key, &ret_pos, false);
        if (r != NULL && r->flag == flag && vlen  == r->vsz
                && memcmp(value, r->value, vlen) == 0)
        {
            if (version != 0)
            {
                // update version
                if ((it->pos & 0xff) == bc->curr)
                {
                    ht_add(bc->curr_tree, key, it->pos, it->hash, ver);
                }
                ht_add(bc->tree, key, it->pos, it->hash, ver);
            }
            suc = true;
            free_record(&r);
            goto SET_FAIL;
        }
        if (r != NULL) free_record(&r);
    }

    int klen = strlen(key);
    DataRecord *r = (DataRecord*)beans_safe_malloc(sizeof(DataRecord) + klen);
    r->ksz = klen;
    memcpy(r->key, key, klen); // safe
    r->vsz = vlen;
    r->value = value;
    r->free_value = false;
    r->flag = flag;
    r->version = ver;
    r->tstamp = time(NULL);

    unsigned int rlen;
    char *rbuf = encode_record(r, &rlen);
    if (rbuf == NULL || (rlen & 0xff) != 0)
    {
        printf("encode_record() failed with %d\n", rlen);
        if (rbuf != NULL) free(rbuf);
        goto SET_FAIL;
    }

    pthread_mutex_lock(&bc->buffer_lock);
    // record maybe larger than buffer
    if (bc->wbuf_curr_pos + rlen > bc->wbuf_size)
    {
        pthread_mutex_unlock(&bc->buffer_lock);
        bc_flush(bc, 0, 0);//just to clear write_buffer so we can enlarge it
        pthread_mutex_lock(&bc->buffer_lock);

        while (rlen > bc->wbuf_size)
        {
            bc->wbuf_size *= 2;
            free(bc->write_buffer);
            bc->write_buffer = (char*)beans_safe_malloc(bc->wbuf_size);
        }
        if (bc->wbuf_start_pos + bc->wbuf_size > settings.max_bucket_size)
        {
            printf("bitcask 0x%x bc_rotate for large record: curr %d -> %d, record size = %d\n",
                    bc->pos, bc->curr, bc->curr+1, rlen);
            bc_rotate(bc);
        }
    }
    memcpy(bc->write_buffer + bc->wbuf_curr_pos, rbuf, rlen); // safe
    int pos = (bc->wbuf_start_pos + bc->wbuf_curr_pos) | bc->curr;
    bc->wbuf_curr_pos += rlen;
    pthread_mutex_unlock(&bc->buffer_lock);

    ht_add(bc->curr_tree, key, pos, hash, ver);
    ht_add(bc->tree, key, pos, hash, ver);
    suc = true;
    free(rbuf);
    free_record(&r);

SET_FAIL:
    pthread_mutex_unlock(&bc->write_lock);
    if (it != NULL) free(it);
    return suc;
}

bool bc_delete(Bitcask *bc, const char *key)
{
    return bc_set(bc, key, "", 0, 0, -1);
}

uint16_t bc_get_hash(Bitcask *bc, const char *pos, unsigned int *count)
{
    return ht_get_hash(bc->tree, pos, count);
}

char *bc_list(Bitcask *bc, const char *pos, const char *prefix)
{
    return ht_list(bc->tree, pos, prefix);
}

uint32_t   bc_count(Bitcask *bc, uint32_t *curr)
{
    uint32_t total = 0;
    ht_get_hash(bc->tree, "@", &total);
    if (NULL != curr && NULL != bc->curr_tree)
    {
        ht_get_hash(bc->curr_tree, "@", curr);
    }
    return total;
}

void bc_stat(Bitcask *bc, uint64_t *bytes)
{
    if (bytes != NULL)
    {
        *bytes = bc->bytes;
    }
}
