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
 *      Hurricane Lee <hurricane1026@gmail.com>
 */

#ifndef __UTIL_H__
#define __UTIL_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <ctype.h>

#ifdef HAVE_MALLOC_H
/* OpenBSD has a malloc.h, but warns to use stdlib.h instead */
#ifndef __OpenBSD__
#include <malloc.h>
#endif
#endif

#ifdef __GNUC__
#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif


inline static void*
_safe_malloc(size_t s, const char *file, int line, const char *func)
{
    void *p = malloc(s);
    if (unlikely(p == NULL))
    {
        printf("Out of memory: %d, %zu bytes in %s (%s:%i)\n", errno, s, func, file, line);
        /*
        * memset will make debug easier
        */
        //memset(p, 0, s);
        exit(1);
    }
    return p;
}

#define beans_safe_malloc(X) _safe_malloc(X, __FILE__, __LINE__, __FUNCTION__)

inline static void*
_try_malloc(size_t s, const char *file, int line, const char *func)
{
    void *p = malloc(s);
    if (unlikely(p == NULL))
    {
        printf("Out of memory: %d, %zu bytes in %s (%s:%i) but continue working.\n", errno, s, func, file, line);
    }
    return p;
}

#define beans_try_malloc(X) _try_malloc(X, __FILE__, __LINE__, __FUNCTION__)

inline static void*
_safe_realloc(void *ptr, size_t s, const char *file, int line, const char *func)
{
    void *p = realloc(ptr, s);
    if (unlikely(p == NULL))
    {
        free(p);
        printf("Realloc failed: %d, %zu bytes in %s (%s:%i)\n", errno, s, func, file, line);
        exit(1);
    }
    return p;
}

#define beans_safe_realloc(X, Y) _safe_realloc(X, Y, __FILE__, __LINE__, __FUNCTION__)

inline static void*
_try_realloc(void *ptr, size_t s, const char *file, int line, const char *func)
{
    void *p = realloc(ptr, s);
    if (unlikely(p == NULL))
    {
        free(p);
        printf("Realloc failed: %d, %zu bytes in %s (%s:%i), but continue working\n", errno, s, func, file, line);
    }
    return p;
}

#define beans_try_realloc(X, Y) _try_realloc(X, Y, __FILE__, __LINE__, __FUNCTION__)

inline static void*
_safe_calloc(size_t num, size_t size, const char *file, int line, const char *func)
{
    void *p = calloc(num, size);
    if (unlikely(p == NULL))
    {
        printf("Calloc failed: %d, %zu bytes in %s (%s:%i)\n", errno, num * size, func, file, line);
        exit(1);
    }
    return p;
}

#define beans_safe_calloc(X, Y) _safe_calloc(X, Y, __FILE__, __LINE__, __FUNCTION__)

inline static void*
_try_calloc(size_t num, size_t size, const char *file, int line, const char *func)
{
    void *p = calloc(num, size);
    if (unlikely(p == NULL))
    {
        printf("Calloc failed: %d, %zu bytes in %s (%s:%i)\n", errno, num * size, func, file, line);
    }
    return p;
}

#define beans_try_calloc(X, Y) _try_calloc(X, Y, __FILE__, __LINE__, __FUNCTION__)

inline static size_t
_check_snprintf(const char *file, int line, const char *func, char *s, size_t n, const char *format, ...)
{
    va_list args;
    size_t result_len;
    va_start (args, format);
    result_len = vsnprintf(s, n, format, args);
    if (unlikely(result_len >= n))
    {
        printf("Truncation: content truncated while calling snprintf \
                in %s (%s:%i), %zu content print to %zu length buffer.", file, func, line, result_len, n);
        exit(1);
    }
    va_end(args);
    return result_len;
}

#define safe_snprintf(BUFFER, N, FORMAT, ...)  _check_snprintf(__FILE__, __LINE__, __FUNCTION__, BUFFER, N, FORMAT, ##__VA_ARGS__)

inline static void*
_check_memcpy(const char *file, int line, const char *func, void *dst, size_t dst_num, const void *src, size_t src_num)
{
    if (unlikely(dst_num < src_num))
    {
        printf("_check_memcpy try to use lower dst buffer: %zu than src size: %zu. \
                in %s (%s:%i).", dst_num, src_num, file, func, line);
        exit(1);
    }
    return memcpy(dst, src, src_num);
}

#define safe_memcpy(DST, DST_NUM, SRC, SRC_NUM) _check_memcpy(__FILE__, __LINE__, __FUNCTION__, DST, DST_NUM, SRC, SRC_NUM)

#define calc_min(a,b) ((a)<(b)?(a):(b))
#define calc_max(a,b) ((a)>(b)?(a):(b))

inline static int safe_strtol(const char *str, int base, long *out)
{
    if (!out)
        return 0;
    errno = 0;
    *out = 0;
    char *endptr;
    long l = strtoul(str, &endptr, base);
    if (errno == ERANGE)
        return 0;
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str))
    {
        *out = l;
        return 1;
    }
    return 0;
}
#endif
