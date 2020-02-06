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


float add(float a, float b) {
    if (a>0) {
        return a;
    } else if (a > b) {
        return b;
    } else {
        return 0.0;
    }
}