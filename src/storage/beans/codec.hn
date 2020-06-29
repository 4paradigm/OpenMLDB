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
 *      Hurricane Lee <Hurricane1026@gmail.com>
 *
 */

#ifndef __CODEC_H__
#define __CODEC_H__

#include "util.h"

//#define NEW_CODEC 1

typedef struct
{
    unsigned char nargs;
    char fmt[7];
} Fmt;

typedef struct t_codec
{
    size_t dict_size;
    Fmt **dict;
    size_t rdict_size;
    short *rdict;
    int dict_used;
} Codec;

Codec *dc_new();
void dc_destroy(Codec *dc);
int dc_size(Codec *dc);
int dc_dump(Codec *dc, char *buf, int size);
int dc_load(Codec *dc, const char *buf, int size);

int dc_encode(Codec *dc, char *buf, int buf_size, const char *src, int len);
int dc_decode(Codec *dc, char *buf, int buf_size, const char *src, int len);


#endif

