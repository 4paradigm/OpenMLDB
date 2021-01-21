//
// compress.h
// Copyright (C) 2017 4paradigm.com
// Author wangbao
// Date 2020-12-04
//


#pragma once

#include "config.h" // NOLINT

#ifdef PZFPGA_ENABLE
#include <gflags/gflags.h>
#include <vector>
#include "base/random.h"
#include "pz.h" // NOLINT

DECLARE_uint32(fpga_env_num);

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
        static std::vector<FPGA_env*> fpga_envs = GetFpgaEnvInternal();
        rtidb::base::Random rand(0xdeadbeef);
        return fpga_envs[rand.Next() % FLAGS_fpga_env_num];
    }

 private:
    static std::vector<FPGA_env*>& GetFpgaEnvInternal() {
        static std::vector<FPGA_env*> fpga_envs;
        for (uint32_t i = 0; i < FLAGS_fpga_env_num; i++) {
            fpga_envs.push_back(gzipfpga_init_titanse());
        }
        return fpga_envs;
    }
};

}  // namespace base
}  // namespace rtidb
#endif
