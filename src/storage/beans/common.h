#ifndef __COMMON_H__
#define __COMMON_H__
#include<stdlib.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_STDBOOL_H
# include <stdbool.h>
#else
# ifndef HAVE__BOOL
#  ifdef __cplusplus
typedef bool _Bool;
#  else
#   define _Bool signed char
#  endif
# endif
# define bool _Bool
# define false 0
# define true 1
# define __bool_true_false_are_defined 1
#endif


#if HAVE_STDINT_H
# include <stdint.h>
#else
typedef unsigned char uint8_t;
#endif

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

/* 64-bit Portable printf */
/* printf macros for size_t, in the style of inttypes.h */
#ifdef _LP64
#define __PRIS_PREFIX "z"
#else
#define __PRIS_PREFIX
#endif

/* Use these macros after a % in a printf format string
   to get correct 32/64 bit behavior, like this:
   size_t size = records.size();
   printf("%"PRIuS"\n", size); */

#define PRIdS __PRIS_PREFIX "d"
#define PRIxS __PRIS_PREFIX "x"
#define PRIuS __PRIS_PREFIX "u"
#define PRIXS __PRIS_PREFIX "X"
#define PRIoS __PRIS_PREFIX "o"





struct settings
{
    int num_threads;        /* number of libevent threads to run */
    size_t item_buf_size;
    int maxconns;
    int port;
    char *inter;
    int verbose;
    float slow_cmd_time;
    int flush_period;
    int flush_limit;
    uint32_t max_bucket_size;
    bool check_file_size;
    bool autolink;
};
extern int daemon_quit;
extern struct settings settings;

void settings_init(void);
#endif
