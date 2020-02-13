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


int first(std::vector<int> list) {
    int res = 0;
    for(int x: list) {
        res += x;
    }
    return res;
}