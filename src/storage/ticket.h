//
// ticket.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-14
//


#ifndef RTIDB_TICKET_H
#define RTIDB_TICKET_H

#include "storage/segment.h"
#include <vector>

namespace rtidb {
namespace storage {

class KeyEntry;

class Ticket {

public:
    Ticket();
    ~Ticket();

    void Push(KeyEntry* entry);
private:
    std::vector<KeyEntry*> entries_;
};

}
}

#endif /* !RTIDB_TICKET_H */
