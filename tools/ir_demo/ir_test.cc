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
#include <vector>
#include <iostream>
#include <string>
double cast(int32_t value) {
    return static_cast<float>(value);
}

//
// int main() {
//    long num1[5] = {1L, 2L, 3L, 4L, 5L};
//    float num2[5] = {6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
//    int num3[5] = {11, 12, 13, 14, 15};
//    float res = udf(num1, num2, num3);
//    printf("res = %.f", res);
//}
