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

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "hint.h"
#include "quicklz.h"
#include "diskmgr.h"
#include "fnv1a.h"
#include "const.h"

#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif


#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "mfile.h"

// for build hint
struct param
{
    int size;
    int curr;
    char *buf;
};

void collect_items(Item *it, void *param)
{
    int ksize = strlen(it->key);
    int length = sizeof(HintRecord) + ksize + 1 - NAME_IN_RECORD;
    struct param *p = (struct param *)param;
    if (p->size - p->curr < length)
    {
        p->size *= 2;
        p->buf = (char*)safe_realloc(p->buf, p->size);
    }

    HintRecord *r = (HintRecord*)(p->buf + p->curr);
    r->ksize = ksize;
    r->pos = it->pos >> 8;
    r->version = it->ver;
    r->hash = it->hash;
        // TODO: check ir
    safe_memcpy(r->key, p->size - p->curr - sizeof(HintRecord) + NAME_IN_RECORD, it->key, r->ksize + 1);

    p->curr += length;
}

void write_hint_file(char *buf, int size, const char *path)
{
    // compress
    char *dst = buf;
    if (strcmp(path + strlen(path) - 4, ".qlz") == 0)
    {
        char *wbuf = (char*)safe_malloc(QLZ_SCRATCH_COMPRESS);
        dst = (char*)safe_malloc(size + 400);
        size = qlz_compress(buf, dst, size, wbuf);
        free(wbuf);
    }

    char tmp[MAX_PATH_LEN];
    safe_snprintf(tmp, MAX_PATH_LEN, "%s.tmp", path);
    FILE *hf = fopen(tmp, "wb");
    if (NULL == hf)
    {
        printf("open %s failed\n", tmp);
        return;
    }
    int n = fwrite(dst, 1, size, hf);
    fclose(hf);
    if (dst != buf) free(dst);

    if (n == size)
    {
        mgr_unlink(path);
        mgr_rename(tmp, path);
    }
    else
    {
        printf("write to %s failed\n", tmp);
    }
}

void build_hint(HTree *tree, const char *hintpath)
{
    struct param p;
    p.size = 1024 * 1024;
    p.curr = 0;
    p.buf = (char*)safe_malloc(p.size);

    ht_visit(tree, collect_items, &p);
    ht_destroy(tree);

    write_hint_file(p.buf, p.curr, hintpath);
    free(p.buf);
}

HintFile *open_hint(const char *path, const char *new_path)
{
    MFile *f = open_mfile(path);
    if (f == NULL)
    {
        return NULL;
    }

    HintFile *hint = (HintFile*) safe_malloc(sizeof(HintFile));
    hint->f = f;
    hint->buf = f->addr;
    hint->size = f->size;

    if (strcmp(path + strlen(path) - 4, ".qlz") == 0 && hint->size > 0)
    {
        char wbuf[QLZ_SCRATCH_DECOMPRESS];
        int size = qlz_size_decompressed(hint->buf);
        char *buf = (char*)safe_malloc(size);
        int vsize = qlz_decompress(hint->buf, buf, wbuf);
        if (vsize != size)
        {
            printf("decompress %s failed: %d < %d, remove it\n", path, vsize, size);
            mgr_unlink(path);
            exit(1);
        }
        hint->size = size;
        hint->buf = buf;
    }

    if (new_path != NULL)
    {
        write_hint_file(hint->buf, hint->size, new_path);
    }

    return hint;
}

void close_hint(HintFile *hint)
{
    if (hint->buf != hint->f->addr && hint->buf != NULL)
    {
        free(hint->buf);
    }
    close_mfile(hint->f);
    free(hint);
}

void scanHintFile(HTree *tree, int bucket, const char *path, const char *new_path)
{
    HintFile *hint = open_hint(path, new_path);
    if (hint == NULL) return;

    printf("scan hint: %s\n", path);

    char *p = hint->buf, *end = hint->buf + hint->size;
    while (p < end)
    {
        HintRecord *r = (HintRecord*) p;
        p += sizeof(HintRecord) - NAME_IN_RECORD + r->ksize + 1;
        if (p > end)
        {
            printf("scan %s: unexpected end, need %ld byte\n", path, p - end);
            break;
        }
        uint32_t pos = (r->pos << 8) | (bucket & 0xff);
        if (check_key(r->key, r->ksize))
        {
            if (r->version > 0)
                ht_add2(tree, r->key, r->ksize, pos, r->hash, r->version);
            else
                ht_remove2(tree, r->key, r->ksize);
        }
    }

    close_hint(hint);
}

int count_deleted_record(HTree *tree, int bucket, const char *path, int *total, bool skipped)
{
    *total = 0;
    HintFile *hint = open_hint(path, NULL);
    if (hint == NULL)
        return 0;

    char *p = hint->buf, *end = hint->buf + hint->size;
    int deleted = 0;
    while (p < end)
    {
        HintRecord *r = (HintRecord*) p;
        p += sizeof(HintRecord) - NAME_IN_RECORD + r->ksize + 1;
        if (p > end)
        {
            printf("scan %s: unexpected end, need %ld byte\n", path, p - end);
            break;
        }
        (*total)++;
        Item *it = ht_get2(tree, r->key, r->ksize);
        //key not exist || not used || (used && deleted && not skipped)
        if (it == NULL || it->pos != ((r->pos << 8) | (unsigned int)bucket) || (it->ver <= 0 && !skipped))
        {
            deleted++;
        }
        if (it) free(it);
    }

    close_hint(hint);
    return deleted;
}
