/*
 * Copyright 2021 4Paradigm
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

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#ifndef SRC_BASE_PORT_H_
#define SRC_BASE_PORT_H_

#undef PLATFORM_IS_LITTLE_ENDIAN
#if __APPLE__
#include <machine/endian.h>
#if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
#define PLATFORM_IS_LITTLE_ENDIAN (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
#endif
#elif defined(OS_SOLARIS)
#include <sys/isa_defs.h>
#ifdef _LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN true
#else
#define PLATFORM_IS_LITTLE_ENDIAN false
#endif
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || defined(OS_DRAGONFLYBSD)
#include <sys/endian.h>
#include <sys/types.h>
#define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#elif defined(OS_HPUX)
#define PLATFORM_IS_LITTLE_ENDIAN false
#else
#include <endian.h>
#endif

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

namespace openmldb {
namespace base {
static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_PORT_H_
