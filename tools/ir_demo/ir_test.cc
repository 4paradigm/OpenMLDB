/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * ir_test.c
 *
 * Author: chenjing
 * Date: 2020/1/20
 *--------------------------------------------------------------------------
 **/
#include <stdbool.h>
#include <vector>
#include <stdio.h>

template <class T>
T add(T a, T b) {
    a += b;
    return a;
}

int int_add(int a, int b) {
    return add(a,b)+1;
}
