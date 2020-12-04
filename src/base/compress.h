//
// compress.h
// Copyright (C) 2017 4paradigm.com
// Author wangbao
// Date 2020-12-04
//


#pragma once

#include "config.h" // NOLINT
#ifdef PZFPGA_ENABLE
#include "pz.h" // NOLINT

namespace rtidb {
namespace base {

class Compress {
 public:
    Compress() = delete;
    ~Compress() = delete;
    Compress(const Compress&) = delete;
    Compress& operator=(const Compress&) = delete;

    // singleton
    static FPGA_env* GetFpgaEnv() {
        static FPGA_env* fpga_env = gzipfpga_init_titanse();
        return fpga_env;
    }
};

}  // namespace base
}  // namespace rtidb
#endif
