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

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "codec.h"
#include "fnv1a.h"
#include "varint.h"

const int MAX_FMT_ARGS = 2;

static inline int fmt_size(Fmt *fmt)
{
    return sizeof(Fmt) + strlen(fmt->fmt) - 7 + 1;
}

const int DEFAULT_DICT_SIZE = 1024;
const int MAX_DICT_SIZE = 16384;

#define RDICT_SIZE(DICT_SIZE) ((DICT_SIZE) * 7 + 1)

Codec *dc_new()
{
    Codec *dc = (Codec*)beans_safe_malloc(sizeof(struct t_codec));

    dc->dict_size = DEFAULT_DICT_SIZE;
    dc->dict = (Fmt**)beans_safe_malloc(sizeof(Fmt*) * dc->dict_size);
    memset(dc->dict, 0, sizeof(Fmt*) * dc->dict_size);

    dc->rdict_size = RDICT_SIZE(dc->dict_size);
    dc->rdict = (short*)beans_safe_malloc(sizeof(short) * dc->rdict_size);
    memset(dc->rdict, 0, sizeof(short) * dc->rdict_size);

    dc->dict_used = 1;

    return dc;
}

int dc_size(Codec *dc)
{
    int i, s = sizeof(int);
    for (i = 1; i < dc->dict_used; ++i)
    {
        s += 1 + fmt_size(dc->dict[i]);
    }
    return s;
}

int dc_dump(Codec *dc, char *buf, int size)
{
    char *orig = buf;
    int i = 0;
    int buf_size = size;
    if (size < (int)sizeof(int)) return -1;
    *(int*)buf = dc->dict_used;
    buf += sizeof(int);
    buf_size -= sizeof(int);

    for (i = 1; i < dc->dict_used; ++i)
    {
        unsigned char s = fmt_size(dc->dict[i]);
        if (buf + s + 1 - orig > size) return -1;
        *(unsigned char*)buf++ = s;
        --buf_size;
        safe_memcpy(buf, buf_size, dc->dict[i], s);
        buf += s;
        buf_size -= s;
    }

    return buf - orig;
}

void dc_rebuild(Codec *dc)
{
    int i;
    dc->rdict_size = RDICT_SIZE(dc->dict_size);
    free(dc->rdict);
    dc->rdict = (short*) beans_safe_malloc(sizeof(short) * dc->rdict_size);
    memset(dc->rdict, 0, sizeof(short) * dc->rdict_size);

    for (i = 1; i < dc->dict_used; ++i)
    {
        uint32_t h = fnv1a(dc->dict[i]->fmt, strlen(dc->dict[i]->fmt)) % dc->rdict_size;
        while (dc->rdict[h] > 0)
        {
            ++h;
            if (h == dc->rdict_size) h = 0;
        }
        dc->rdict[h] = i;
    }
}

void dc_enlarge(Codec *dc)
{
    dc->dict_size = calc_min(dc->dict_size * 2, MAX_DICT_SIZE);
    dc->dict = (Fmt**)beans_safe_realloc(dc->dict, sizeof(Fmt*) * dc->dict_size);
    printf("enlare codec to %zu\n", dc->dict_size);
    dc_rebuild(dc);
}

int dc_load(Codec *dc, const char *buf, int size)
{
    //const char *orig = buf;
    int i;
    int offset = 0;
    if (dc == NULL) return -1;
    int used = *(int*)buf;
    buf += sizeof(int);
    offset += sizeof(int);
    if (offset > size) return -1;
    if (used > MAX_DICT_SIZE)
    {
        printf("number of formats overflow: %d > %d\n", used, MAX_DICT_SIZE);
        return -1;
    }
    unsigned int dict_size = calc_min(used * 2, MAX_DICT_SIZE);
    if (dc->dict_size < dict_size)
    {
        dc->dict = (Fmt**) beans_safe_realloc(dc->dict, sizeof(Fmt*) * dict_size);
        dc->dict_size = dict_size;
    }

    dc->dict_used = 1;
    for (i = 1; i < used; ++i)
    {
        int s = *(unsigned char*)buf++;
        offset += sizeof(unsigned char);
        if (offset > size) return -1;
        dc->dict[i] = (Fmt*)beans_try_malloc(s);
        if (dc->dict[i] == NULL)
        {
            printf("try_malloc failed: %d\n", s);
            return -1;
        }
        dc->dict_used++;
        memcpy(dc->dict[i], buf, s); // safe
        buf += s;
        offset += sizeof(char) * s;
        if (offset > size) return -1;
    }

    dc_rebuild(dc);

    return 0;
}

void dc_destroy(Codec *dc)
{
    int i;
    if (dc == NULL) return;

    if (dc->rdict) free(dc->rdict);
    for (i = 1; i < dc->dict_used; i++)
        free(dc->dict[i]);
    if (dc->dict) free(dc->dict);
    free(dc);
}

static inline int parse_fmt(const char *src, int len, char *fmt, int *flen, int32_t *args)
{
    int m = 0; //narg
    bool hex[20];
    char num[20][10];
    const char *p = src, *q = src + len;
    char *dst = fmt;
    while(p < q)
    {
        if (*p == '%' || *p == '@' || *p == ':')   // not supported format
        {
            return 0;
        }
        if (((*p >= '1' && *p <= '9') || (*p >= 'a' && *p <= 'f')) && m < MAX_FMT_ARGS + 1)
        {
            char *nd = num[m];
            hex[m] = false;
            while(p < q && ((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f')))
            {
                if (*p >= 'a' && *p <= 'f') hex[m] = true;
                *nd++ = *p++;
                if ((hex[m] && nd-num[m] >= 8) || (!hex[m] && nd-num[m] >= 9))
                {
                    break;
                }
            }
            // 8digit+1hex, pop it
            if (hex[m] && nd-num[m]==9)
            {
                --nd;
                --p;
                hex[m] = false;
            }
            *nd = 0;
            if (hex[m] && nd - num[m] >= 4)
            {
                *dst++ = '%';
                *dst++ = 'x';
                args[m] = strtol(num[m], NULL, 16);
                ++m;
            }
            else if (!hex[m] && nd - num[m] >= 3)
            {
                *dst++ = '%';
                *dst++ = 'd';
                args[m] = atoi(num[m]);
                ++m;
            }
            else
            {
                safe_memcpy(dst, 255, num[m], nd - num[m]);
                dst += nd - num[m];
            }
        }
        else
        {
            *dst++ = *p++;
        }
    }
    *dst = 0; // ending 0
    *flen = dst - fmt;
    return m;
}

static inline int parse_fmt_new(const char *src, int len, char *fmt, int *flen, uint64_t *args)
{
    int m = 0; //narg
    bool hex[20];
    char num[20][10];
    const char *p = src, *q = src + len;
    char *dst = fmt;
    while(p < q)
    {
        if (*p == '%' || *p == '@' || *p == ':')   // not supported format
        {
            return 0;
        }
        if ((*p >= '1' && *p <= '9') || (*p >= 'a' && *p <= 'f'))
        {
            char *nd = num[m];
            hex[m] = false;
            while(p < q && ((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f')))
            {
                if (*p >= 'a' && *p <= 'f') hex[m] = true;
                *nd++ = *p++;
                if ((hex[m] && nd-num[m] >= 16) || (!hex[m] && nd-num[m] >= 18))
                {
                    break;
                }
            }
            // 8digit+1hex, pop it
            if (hex[m] && nd-num[m]==9)
            {
                --nd;
                --p;
                hex[m] = false;
            }
            *nd = 0;
            if (hex[m] && nd - num[m] >= 4)
            {
                *dst++ = '%';
                *dst++ = 'l';
                *dst++ = 'l';
                *dst++ = 'x';
                args[m] = strtoll(num[m], NULL, 16);
                ++m;
            }
            else if (!hex[m] && nd - num[m] >= 3)
            {
                *dst++ = '%';
                *dst++ = 'l';
                *dst++ = 'l';
                *dst++ = 'd';
                args[m] = strtoll(num[m], NULL, 10);
                ++m;
            }
            else
            {
                safe_memcpy(dst, 255, num[m], nd - num[m]);
                dst += nd - num[m];
            }
        }
        else
        {
            *dst++ = *p++;
        }
    }
    *dst = 0; // ending 0
    *flen = dst - fmt;
    return m;
}

static inline int dc_encode_key_with_fmt(int idx, char *buf, int buf_size, int32_t *args, int narg)
{
    int intlen = encode_varint_old(idx, buf);
    safe_memcpy(buf + intlen, buf_size - intlen, args, sizeof(int32_t)*narg);
    return intlen + narg * sizeof(int32_t);
}

static inline int dc_encode_key_with_fmt_new(int idx, char *buf, int buf_size, uint64_t *args, int narg)
{
    int len = encode_varint_old(idx, buf);
    int i;
    for (i = 0; i < narg; i++)
    {
        int l = encode_varint((uint64_t)args[i], buf + len);
        len += l;
    }
    return len;
}


static inline int dc_decode_key_with_fmt(Codec *dc, char *buf, int buf_size, const char *src, int len)
{
    if (len < 5)
        return 0;
    int intlen;
    int idx = decode_varint_old(src, &intlen);
    int32_t *args = (int32_t*)(src + intlen);
    Fmt *f = dc->dict[idx];

    if (f == NULL)
    {
        int key_buf_len = sizeof(char) * len * 2 + 1;
        char *key_hex_buf = (char*)beans_safe_malloc(key_buf_len);
        *(key_hex_buf + key_buf_len - 1) = 0;
        printf("invalid fmt index: %d\n", idx);
        int i;
        for (i = 0; i < len; ++i)
        {
            sprintf(key_hex_buf + 2 * i, "%x", src[i]); //safe
        }
        printf("invalid key: %s\n", key_hex_buf);
        free(key_hex_buf);
        return 0;
    }
    int nlen = f->nargs * sizeof(int32_t) + ((char *)args - src);
    if (len != nlen)
    {
        printf("invalid length of key: %d != %d\n", len, nlen);
        return 0;
    }
    int rlen = 0;
    switch(f->nargs)
    {
        case 1:
            rlen = safe_snprintf(buf, buf_size, f->fmt, args[0]);
            break;
        case 2:
            rlen = safe_snprintf(buf, buf_size, f->fmt, args[0], args[1]);
            break;
        case 3:
            rlen = safe_snprintf(buf, buf_size, f->fmt, args[0], args[1], args[2]);
            break;
        default:
            ;
    }
    return rlen;
}

static inline int dc_decode_key_with_fmt_new(Codec *dc, char *buf, int buf_size, const char *src, int len)
{
    int intlen;
    char*p = (char*) src;
    int idx = decode_varint(p, &intlen);
    //printf("index= %d\n", idx);
    Fmt *f = dc->dict[idx];
    if (f == NULL)
    {
        printf("invalid fmt index: %d\n", idx);
        printbuf(src, len);
        return 0;
    }
    uint64_t args[5];

    int i;
    p += intlen;
    for (i = 0; i<f->nargs; i++)
    {
       args[i] = decode_varint(p, &intlen);
       p += intlen;
    }
    if (p-src != len)
    {
        printf("invalid length of key: %d != %ld\n", len, p-src);
        printbuf(src, len);
        return 0;
    }
    int rlen = 0;
    switch(f->nargs)
    {
        case 1:
            rlen = safe_snprintf(buf, buf_size, f->fmt, args[0]);
            break;
        case 2:
            rlen = safe_snprintf(buf, buf_size, f->fmt, args[0], args[1]);
            break;
        case 3:
            rlen = safe_snprintf(buf, buf_size, f->fmt, args[0], args[1], args[2]);
            break;
        default:
            ;
    }
    return rlen;
}

int dc_encode(Codec *dc, char *buf, int buf_size, const char *src, int len)
{
    char fmt[255];
    int flen = 0;

#ifndef NEW_ENCODE
    int args[10];
    int narg = parse_fmt(src, len, fmt, &flen, args);
#else
    uint64_t args[10];
    int narg = parse_fmt_new(src, len, fmt, &flen, args);
#endif

    if (dc && len > 6 && len < 100 && src[0] > 0 && narg > 0 && narg <= MAX_FMT_ARGS)
    {
        Fmt **dict = dc->dict;
        uint32_t h = fnv1a(fmt, flen) % dc->rdict_size;
        // test hash collision
        while (dc->rdict[h] > 0 && strcmp(fmt, dict[dc->rdict[h]]->fmt) != 0)
        {
            ++h;
            if (h == dc->rdict_size) h = 0;
        }
        int rh = dc->rdict[h];
        if (rh == 0)
        {
            if ((unsigned int)(dc->dict_used) < dc->dict_size)
            {
                dict[dc->dict_used] = (Fmt*) beans_safe_malloc(sizeof(Fmt) + flen - 7 + 1);
                dict[dc->dict_used]->nargs = narg;
                memcpy(dict[dc->dict_used]->fmt, fmt, flen + 1);
                printf("new fmt %d: %s <= %s\n", dc->dict_used, fmt, src);
                dc->rdict[h] = rh = dc->dict_used++;
                if ((unsigned int)(dc->dict_used) == dc->dict_size && dc->dict_size < MAX_DICT_SIZE)
                {
                    dc_enlarge(dc);
                }
            }
            else
            {
                printf("not captched fmt: %s <= %s\n", fmt, src);
                dc->rdict[h] = rh = -1; // not again
            }
        }
        if (rh > 0)

#ifndef NEW_ENCODE
            return dc_encode_key_with_fmt(rh, buf, buf_size, args, narg);
#else
            return dc_encode_key_with_fmt_new(rh, buf, buf_size, args, narg);
#endif
    }
    safe_memcpy(buf, buf_size, src, len);
    return len;
}

int dc_decode(Codec *dc, char *buf, int buf_size, const char *src, int len)
{
    if (src[0] < 0)
#ifndef NEW_ENCODE
        return dc_decode_key_with_fmt(dc, buf, buf_size, src, len);
#else
        return dc_decode_key_with_fmt_new(dc, buf, buf_size, src, len);
#endif

    safe_memcpy(buf, buf_size, src, len);
    buf[len] = 0;
    return len;
}
