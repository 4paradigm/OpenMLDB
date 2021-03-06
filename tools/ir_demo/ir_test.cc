/*
 * Copyright (c) 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
