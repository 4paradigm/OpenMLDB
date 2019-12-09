/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window.cc
 *
 * Author: chenjing
 * Date: 2019/12/6
 *--------------------------------------------------------------------------
 **/
#include "storage/window.h"
namespace fesql {
namespace storage {


ColumnStringIteratorImpl::ColumnStringIteratorImpl(
    const IteratorImpl<Row> &impl, int32_t str_field_offset,
    int32_t next_str_field_offset, int32_t str_start_offset)
    : ColumnIteratorImpl<::fesql::storage::StringRef>(impl, 0u),
      str_field_offset_(str_field_offset),
      next_str_field_offset_(next_str_field_offset),
      str_start_offset_(str_start_offset) {}

}
}
