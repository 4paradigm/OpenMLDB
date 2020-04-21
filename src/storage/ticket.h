//
// ticket.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-14
//

#ifndef SRC_STORAGE_TICKET_H_
#define SRC_STORAGE_TICKET_H_

#include <vector>
#include "storage/segment.h"

namespace rtidb {
namespace storage {

class KeyEntry;

class Ticket {
 public:
    Ticket();
    ~Ticket();

    void Push(KeyEntry* entry);
    void Pop();

 private:
    std::vector<KeyEntry*> entries_;
};

}  // namespace storage
}  // namespace rtidb

#endif  // SRC_STORAGE_TICKET_H_
