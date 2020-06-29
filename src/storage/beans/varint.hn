#include<stdio.h>
#include <stdbool.h>
#include <stdint.h>

static inline bool is_int(const char *p)
{
    return (*p) & 0x80;
}

void printbuf(const char *buf, int n)
{
    int i;
    printf("%d[", n);
    for (i = 0; i< n;i++)
       printf("(%x,%x)", buf[i] & 0x7f, buf[i] & 0x80);
    printf("]\n");
}

int encode_varint(uint64_t n, char *buf)
{
    char *p = buf;
	if (n < 64)
	{
	    *buf = n + 0x80;
        return 1;
	}

    *buf = (n & 0x3f) + 0xc0;
    n >>= 6;

    while(n > 0)
    {
        p ++;
        *p = (n & 0x7f)  + 0x80;
        n >>= 7;
    }
    *p &=  0x7f;
    return (p-buf+1);
}

uint64_t decode_varint(const char *src, int *len)
{
    char *p  = (char *)src;
    uint64_t n = (*p) & 0x7f;
    *len = 1;
    if (n < 64)
    {
        return n;
    }
    n &= 0x3f;
    ++p;
    uint64_t multi = 1 << 6;
    do
    {
        int v = (*p) & 0x7f;
        n += multi * v;
        multi <<= 7;
    } while ((*p++ & 0x80) != 0);
    *len = (p - src);
    return n;
}

int encode_varint_old(int n, char *buf)
{
	int len;
	if (n < 64)
	{
	    len = 1;
	    *buf = -n;
	}
	else
	{
	    len = 2;
	    *buf = -(n & 0x3f) - 64;
	    *(unsigned char*)(buf + 1) = n >> 6;
	}
	return len;
}

int decode_varint_old(const char *src, int *len)
{
    int n = -*src;
    *len = 1;
    if (n >= 64)
    {
        n -= 64;
        n += (*(unsigned char*)(src + 1)) << 6;
       *len = 2;
    }
    return n;
}

