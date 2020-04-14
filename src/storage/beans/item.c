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
 *
 */

#include "beansdb.h"
#include "hstore.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif


#include "util.h"
#include "log.h"

#define MAX_ITEM_FREELIST_LENGTH 4000
#define INIT_ITEM_FREELIST_LENGTH 500

static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes, char *suffix, uint8_t *nsuffix);

static item **freeitem;
static int freeitemtotal;
static int freeitemcurr;

extern HStore *store;

void item_init(void)
{
    freeitemtotal = INIT_ITEM_FREELIST_LENGTH;
    freeitemcurr  = 0;

    freeitem = (item **)safe_malloc(sizeof(item *) * freeitemtotal);
    return;
}

/*
 * Returns a item buffer from the freelist, if any. Sholud call
 * item_from_freelist for thread safty.
 * */
item *do_item_from_freelist(void)
{
    item *s;

    if (freeitemcurr > 0)
    {
        s = freeitem[--freeitemcurr];
    }
    else
    {
        /* If try_malloc fails, let the logic fall through without spamming
         * STDERR on the server. */
        s = (item *)try_malloc(settings.item_buf_size);
        if (s) memset(s, 0, settings.item_buf_size);
    }

    return s;
}

/*
 * Adds a item to the freelist. Should call
 * item_add_to_freelist for thread safty.
 */
int do_item_add_to_freelist(item *it)
{
    if (freeitemcurr < freeitemtotal)
    {
        freeitem[freeitemcurr++] = it;
        return 0;
    }
    else
    {
        if (freeitemtotal >= MAX_ITEM_FREELIST_LENGTH)
        {
            return 1;
        }
        /* try to enlarge free item buffer array */
        item **new_freeitem = (item **)try_realloc(freeitem, sizeof(item *) * freeitemtotal * 2);
        if (new_freeitem)
        {
            freeitemtotal *= 2;
            PDLOG(INFO, "freeitemtotal doubled to %d", freeitemtotal);
            freeitem = new_freeitem;
            freeitem[freeitemcurr++] = it;
            return 0;
        }
    }
    return 1;
}

/**
 * Generates the variable-sized part of the header for an object.
 *
 * key     - The key
 * nkey    - The length of the key
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes,
                               char *suffix, uint8_t *nsuffix)
{
    /* suffix is defined at 40 chars elsewhere.. */
    *nsuffix = (uint8_t)safe_snprintf(suffix, 40, " %d %d\r\n", flags, nbytes - 2);
    return sizeof(item) + nkey + *nsuffix + nbytes;
}

/*
 * alloc a item buffer, and init it.
 */
item *item_alloc1(char *key, const size_t nkey, const int flags, const int nbytes)
{
    uint8_t nsuffix;
    item *it;
    char suffix[40];
    size_t ntotal = item_make_header(nkey + 1, flags, nbytes, suffix, &nsuffix);

    if (ntotal > settings.item_buf_size)
    {
        it = (item *)try_malloc(ntotal);
        if (it == NULL)
        {
            return NULL;
        }
        memset(it, 0, ntotal);
        if (settings.verbose > 1)
        {
            PDLOG(DEBUG, "alloc a item buffer from try_malloc.");
        }
    }
    else
    {
        it = item_from_freelist();
        if (it == NULL)
        {
            return NULL;
        }
        if (settings.verbose > 1)
        {
            PDLOG(DEBUG, "alloc a item buffer from freelist.");
        }
    }

    it->nkey = nkey;
    it->nbytes = nbytes;
    strcpy(ITEM_key(it), key);
    memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix); // safe
    it->nsuffix = nsuffix;
    return it;
}

/*
 * free a item buffer. here 'it' must be a full item.
 */

int item_free(item *it)
{
    size_t ntotal = 0;
    if (NULL == it)
        return 0;

    /* ntotal may be wrong, if 'it' is not a full item. */
    ntotal = ITEM_ntotal(it);
    if (ntotal > settings.item_buf_size)
    {
        if (settings.verbose > 1)
        {
            PDLOG(ERROR, "ntotal: %"PRIuS", use free() directly.", ntotal);
        }
        free(it);
    }
    else
    {
        if (0 != item_add_to_freelist(it))
        {
            if (settings.verbose > 1)
            {
                PDLOG(ERROR, "ntotal: %"PRIuS", add a item buffer to freelist fail, use free() directly.", ntotal);
            }
            free(it);
        }
        else
        {
            if (settings.verbose > 1)
            {
                PDLOG(ERROR, "ntotal: %"PRIuS", add a item buffer to freelist.", ntotal);
            }
        }
    }
    return 0;
}

/* if return item is not NULL, free by caller */
item *item_get(char *key, unsigned int nkey)
{
    item *it = NULL;
    unsigned int vlen;
    uint32_t flag;
    char *value = hs_get(store, key, &vlen, &flag);
    if (value)
    {
        it = item_alloc1(key, nkey, flag, vlen + 2);
        if (it)
        {
            memcpy(ITEM_data(it), value, vlen); // safe
            memcpy(ITEM_data(it) + vlen, "\r\n", 2); // safe
        }
        free(value);
    }
    return it;
}
