/*
 * rtidb.cc
 * Copyright 2017 elasticlog <elasticlog01@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include "storage/memtable.h"
#include "storage/dbformat.h"
#include "util/comparator.h"
#include "version.h"

int main(int argc, char* argv[]) {
  printf("version %d.%d\n",RTIDB_VERSION_MAJOR, RTIDB_VERSION_MINOR);
  const rtidb::Comparator* com = rtidb::BytewiseComparator();
  rtidb::InternalKeyComparator ic(com);
  rtidb::MemTable* table = new rtidb::MemTable(ic);
  table->Ref();
  rtidb::Slice key("test");
  rtidb::Slice value("value");
  rtidb::SequenceNumber seq = 10;
  table->Add(seq, rtidb::kTypeValue, key, value);
  table->Unref();
  return 0;
}

/* vim: set expandtab ts=2 sw=2 sts=2 tw=100: */
