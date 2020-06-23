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

int GetDate(int64_t date, int32_t* year, int32_t* month, int32_t* day) {
    *day = date & 255;
    date = date << 4;
    //    *month = 1 + (date & 0x0000FF);
    //    *year = 1900 + (date >> 8);
    return date;
}

int YearTime() {
    time_t t;
    time(&t);
    struct tm* tmp_time = localtime(&t);
    char s[100];
    strftime(s, sizeof(s), "%04Y%02m%02d %H:%M:%S", tmp_time);
    printf("%d: %s\n", (int)t, s);
    return 0;
}
//
// int main() {
//    long num1[5] = {1L, 2L, 3L, 4L, 5L};
//    float num2[5] = {6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
//    int num3[5] = {11, 12, 13, 14, 15};
//    float res = udf(num1, num2, num3);
//    printf("res = %.f", res);
//}
