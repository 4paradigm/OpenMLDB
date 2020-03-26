//
// memcomparable_format.h  
// Copyright (C) 2017 4paradigm.com
// Author wangbao
// Date 2020-3-24

/*
   Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/


#include <stdlib.h>
#include<algorithm>
#include<float.h>
#include<string.h>

typedef unsigned char  uchar;  /* Short for unsigned char */
typedef unsigned short  ushort;        
typedef unsigned int  uint;

#define FLT_EXP_DIG (sizeof(float) * 8 - FLT_MANT_DIG)
#define DBL_EXP_DIG (sizeof(double) * 8 - DBL_MANT_DIG)
#define RDB_ESCAPE_LENGTH 9

#pragma once
namespace rtidb {
namespace base {

    static void RdbSwapFloatBytes(uchar *const dst, const uchar *const src) {
        dst[0] = src[3];
        dst[1] = src[2];
        dst[2] = src[1];
        dst[3] = src[0];
    }

    static void RdbSwapDoubleBytes(uchar *const dst, const uchar *const src) {
        dst[0] = src[7];
        dst[1] = src[6];
        dst[2] = src[5];
        dst[3] = src[4];
        dst[4] = src[3];
        dst[5] = src[2];
        dst[6] = src[1];
        dst[7] = src[0];
    }

    static void CopyInteger(const uchar* from, int length, 
            bool is_unsigned, uchar *to) {
        const int sign_byte= from[length - 1];
        if (is_unsigned)
            to[0] = sign_byte;
        else
            to[0] = static_cast<char>(sign_byte ^ 128); // Reverse the sign bit.
        for (int i = 1, j = length - 2; i < length; ++i, --j)
            to[i] = from[j];
    }
    
    static int MakeSortKeyInteger(const void* from, uint length, 
            bool unsigned_flag, void *to) {
        if (from == nullptr || length < 2) {
            return -1;      
        }
        uchar* ptr = (uchar*)from;
        uchar* uto = (uchar*)to;
        CopyInteger(ptr, length, unsigned_flag, uto);
        return 0;
    }

    static int MakeSortKeyFloat(const void* from, uint length, void *to) {
        if (from == nullptr || length != sizeof(float)) {
            return -1;      
        }
        const uchar* ptr = (uchar*)from;
        float nr;
        memcpy(&nr, ptr, length);

        uchar *tmp = (uchar*)to;
        if (nr == (float)0.0) {
            /* Change to zero string */
            tmp[0] = (uchar)128;
            memset(tmp + 1, 0, length - 1);
        }
        else {
            RdbSwapFloatBytes(tmp, ptr);
            if (tmp[0] & 128) {
                /* Negative */
                uint i;
                for (i = 0; i < sizeof(nr); i++)
                    tmp[i] = (uchar)(tmp[i] ^ (uchar)255);
            } else {
                ushort exp_part = (((ushort)tmp[0] << 8) | (ushort)tmp[1] |
                        (ushort)32768);
                exp_part += (ushort)1 << (16 - 1 - FLT_EXP_DIG);
                tmp[0] = (uchar)(exp_part >> 8);
                tmp[1] = (uchar)exp_part;
            }
        }
    }

    /*
     * ** functions to change a double or float to a sortable string
     * ** The following should work for IEEE
     * */
    static void ChangeDoubleForSort(double nr, void *to) {
        uchar *tmp = (uchar*)to;
        if (nr == 0.0) {/* Change to zero string */
            tmp[0] = (uchar)128;
            memset(tmp + 1, 0, sizeof(nr)-1);
        } else {
            uchar *ptr = (uchar*) &nr;
            RdbSwapDoubleBytes(tmp, ptr);
            if (tmp[0] & 128) {/* Negative */
                uint i;
                for (i = 0; i < sizeof(nr); i++)
                    tmp[i] = tmp[i] ^ (uchar)255;
            } else {/* Set high and move exponent one up */
                ushort exp_part = (((ushort)tmp[0] << 8) | (ushort) tmp[1] |
                        (ushort)32768);
                exp_part += (ushort)1 << (16 - 1 - DBL_EXP_DIG);
                tmp[0] = (uchar)(exp_part >> 8);
                tmp[1] = (uchar)exp_part;
            }
        }
    }

    /* The following should work for IEEE */
    static int MakeSortKeyDouble(const void* from, uint length, void *to) {
        const uchar* ptr = (uchar*)from;
        double nr;
        memcpy(&nr, ptr, length);
        if (length < 8) {
            uchar buff[8];
            ChangeDoubleForSort(nr, buff);
            memcpy(to, buff, length);
        } else
            ChangeDoubleForSort(nr, to);
        return 0;
    }

    /*
       This is the new algorithm.  Similarly to the legacy format the input
       is split up into N-1 bytes and a flag byte is used as the Nth byte
       in the output.

       - If the previous segment needed any padding the flag is set to the
       number of bytes used (0..N-2).  0 is possible in the first segment
       if the input is 0 bytes long.
       - If no padding was used and there is no more data left in the input
       the flag is set to N-1
       - If no padding was used and there is still data left in the input the
       flag is set to N.

       For N=9, the following input values encode to the specified
       outout (where 'X' indicates a byte of the original input):
       - 0 bytes  is encoded as 0 0 0 0 0 0 0 0 0
       - 1 byte   is encoded as X 0 0 0 0 0 0 0 1
       - 2 bytes  is encoded as X X 0 0 0 0 0 0 2
       - 7 bytes  is encoded as X X X X X X X 0 7
       - 8 bytes  is encoded as X X X X X X X X 8
       - 9 bytes  is encoded as X X X X X X X X 9 X 0 0 0 0 0 0 0 1
       - 10 bytes is encoded as X X X X X X X X 9 X X 0 0 0 0 0 0 2
     */
    static void PackVariableFormat(
            const void *src,  // The data to encode
            size_t src_len,    // The length of the data to encode
            void **dst) {     // The location to encode the data 
        const uchar* usrc = (uchar*)src;
        uchar *ptr = (uchar*)*dst;

        for (;;) {
            // Figure out how many bytes to copy, copy them and adjust pointers
            const size_t copy_len = std::min((size_t)RDB_ESCAPE_LENGTH - 1, src_len);
            memcpy(ptr, usrc, copy_len);
            ptr += copy_len;
            usrc += copy_len;
            src_len -= copy_len;

            // Are we at the end of the input?
            if (src_len == 0) {
                // pad with zeros if necessary;
                const size_t padding_bytes = RDB_ESCAPE_LENGTH - 1 - copy_len;
                if (padding_bytes > 0) {
                    memset(ptr, 0, padding_bytes);
                    ptr += padding_bytes;
                }
                // Put the flag byte (0 - N-1) in the output
                *(ptr++) = (uchar)copy_len;
                break;
            }
            // We have more data - put the flag byte (N) in and continue
            *(ptr++) = RDB_ESCAPE_LENGTH;
        }
        *dst = ptr;
    }
    
    static int UnpackInteger(const void* from, uint length, bool unsigned_flag, void *to) {
        const uchar *ufrom = (uchar*)from;
        uchar* uto = (uchar*)to;
        const int sign_byte = ufrom[0];
        if (unsigned_flag) {
            uto[length - 1] = sign_byte;
        } else {
            uto[length - 1] = static_cast<char>(sign_byte ^ 128);  // Reverse the sign bit.
        }
        for (uint i = 0, j = length - 1; i < length - 1; ++i, --j) 
            uto[i] = ufrom[j];
        return 0;
    }
    
    static int UnpackFloatingPoint(const void *src, 
            const size_t size,
            const int exp_digit, 
            const uchar *const zero_pattern,
            const uchar *const zero_val, 
            void (*swap_func)(uchar*, const uchar*), 
            void* dst) {
        const uchar *const from = (uchar*)src;
        uchar *const udst = (uchar*)dst;
        if (from == nullptr) {
            return -1;
        }
        /* Check to see if the value is zero */
        if (memcmp(from, zero_pattern, size) == 0) {
            memcpy(udst, zero_val, size);
            return 0;
        }
        // use a temporary buffer to make byte-swapping easier later
        uchar tmp[8];
        memcpy(tmp, from, size);
        if (tmp[0] & 0x80) {
            // If the high bit is set the original value was positive so
            // remove the high bit and subtract one from the exponent.
            ushort exp_part = ((ushort)tmp[0] << 8) | (ushort)tmp[1];
            exp_part &= 0x7FFF;                             // clear high bit;
            exp_part -= (ushort)1 << (16 - 1 - exp_digit);  // subtract from exponent
            tmp[0] = (uchar)(exp_part >> 8);
            tmp[1] = (uchar)exp_part;
        } else {
            // Otherwise the original value was negative and all bytes have been
            // negated.
            for (size_t ii = 0; ii < size; ii++) tmp[ii] ^= 0xFF;
        }
        // On little-endian, swap the bytes around
        swap_func(udst, tmp);
        return 0;
    }

    /*
       Function of type rdb_index_field_unpack_t

       Unpack a float by doing the reverse action of Field_float::make_sort_key
       (sql/field.cc).  Note that this only works on IEEE values.
       Note also that this code assumes that NaN and +/-Infinity are never
       allowed in the database.
    */
    static int UnpackFloat(const void *src, void* dst) {
        static float zero_val = 0.0;
        static const uchar zero_pattern[4] = {128, 0, 0, 0};
        return UnpackFloatingPoint(src, sizeof(float), FLT_EXP_DIG,
                zero_pattern, (const uchar*)&zero_val,
                RdbSwapFloatBytes, dst);
    }

    /*
       Function of type rdb_index_field_unpack_t

       Unpack a double by doing the reverse action of ChangeDoubleForSort
       (sql/filesort.cc).  Note that this only works on IEEE values.
       Note also that this code assumes that NaN and +/-Infinity are never
       allowed in the database.
    */
    static int UnpackDouble(const void *src, void* dst) {
        static double zero_val = 0.0;
        static const uchar zero_pattern[8] = {128, 0, 0, 0, 0, 0, 0, 0};
        return UnpackFloatingPoint(src, sizeof(double), DBL_EXP_DIG,
                zero_pattern, (const uchar*)&zero_val,
                RdbSwapDoubleBytes, dst);
    }

    /*
       Read the next @param size bytes. 
    */
    static const uchar *Read(const uint size, const uchar **usrc) {
        const uchar *res;
        res = *usrc;
        *usrc += size;
        return res;
    }

    /*
       Calculate the number of used bytes in the chunk and whether this is the
       last chunk in the input.  This is based on the new format - see
       pack_variable_format.
    */
    static uint CalcUnpackVariableFormat(uchar flag, bool *done) {
        // Check for invalid flag values
        if (flag > RDB_ESCAPE_LENGTH) {
            return (uint)-1;
        }
        // Values from 1 to N-1 indicate this is the last chunk and that is how
        // many bytes were used
        if (flag < RDB_ESCAPE_LENGTH) {
            *done = true;
            return flag;
        }
        // A value of N means we used N-1 bytes and had more to go
        *done = false;
        return RDB_ESCAPE_LENGTH - 1;
    }

    /*
       Function of type rdb_index_field_unpack_t
    */
    static int UnpackBinaryVarchar(const void *src, void *dst, int32_t *size) {
        const uchar *usrc = (uchar*)src;
        uchar *udst = (uchar*)dst;
        const uchar *ptr;
        size_t len = 0;
        bool finished = false;
        /* Decode the length-emitted encoding here */
        while ((ptr = Read(RDB_ESCAPE_LENGTH, &usrc))) {
            uint used_bytes = CalcUnpackVariableFormat(ptr[RDB_ESCAPE_LENGTH - 1], &finished);
            if (used_bytes == (uint)-1) {
                return -1;  
            }
            /*
               Now, we need to decode used_bytes of data and append them to the value.
            */
            memcpy(udst, ptr, used_bytes);
            udst += used_bytes;
            len += used_bytes;
            if (finished) {
                break;
            }
        }
        if (!finished) {
            return -1;
        }
        /* Save the length */
        *size = len;
        return 0;
    }

}
}
