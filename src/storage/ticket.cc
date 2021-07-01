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

#include "storage/ticket.h"

namespace openmldb {
namespace storage {

Ticket::Ticket() {}

Ticket::~Ticket() {
    std::vector<KeyEntry*>::iterator it = entries_.begin();
    for (; it != entries_.end(); ++it) {
        (*it)->UnRef();
    }
}

void Ticket::Push(KeyEntry* entry) {
    if (entry == NULL) {
        return;
    }
    entry->Ref();
    entries_.push_back(entry);
}

void Ticket::Pop() {
    if (!entries_.empty()) {
        KeyEntry* entry = entries_.back();
        entries_.pop_back();
        entry->UnRef();
    }
}

}  // namespace storage
}  // namespace openmldb
