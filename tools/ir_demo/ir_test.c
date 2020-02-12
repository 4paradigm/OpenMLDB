/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * ir_test.c
 *
 * Author: chenjing
 * Date: 2020/1/20
 *--------------------------------------------------------------------------
 **/
#include <stdbool.h>
#include <stdio.h>


int add(int x, int y) {
    if (x > 1) {
        return x+y;
    } else if (y > 1) {
        return x-y;
    } else {
        return x*y;
    }
    return 0;
}