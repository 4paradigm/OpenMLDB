/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * default_udf_library.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_UDF_DEFAULT_UDF_LIBRARY_H_
#define SRC_UDF_DEFAULT_UDF_LIBRARY_H_
#include "udf/udf_registry.h"

namespace fesql {
namespace udf {

class DefaultUDFLibrary : public UDFLibrary {
 public:
 	DefaultUDFLibrary() { Init(); }

 	static DefaultUDFLibrary& instance() {
 		return instance_;
 	}

 private:
 	void Init();

	static DefaultUDFLibrary instance_;
};


}  // namespace udf
}  // namespace fesql


#endif  // SRC_UDF_DEFAULT_UDF_LIBRARY_H_