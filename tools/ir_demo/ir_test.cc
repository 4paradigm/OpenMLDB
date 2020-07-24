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

char* memcpy_test(char* src1, char* src2, size_t l1, size_t l2) {
    char *dist = reinterpret_cast<char* >(malloc(l1+l2));
    memcpy(dist, src1, l1);
    memcpy(dist+l1, src2, l2);
    return dist;

//    std::cout << std::string(dist, l1+l2);
}

//
// int main() {
//    long num1[5] = {1L, 2L, 3L, 4L, 5L};
//    float num2[5] = {6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
//    int num3[5] = {11, 12, 13, 14, 15};
//    float res = udf(num1, num2, num3);
//    printf("res = %.f", res);
//}
