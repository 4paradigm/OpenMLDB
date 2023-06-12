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

#ifndef SRC_BASE_FILE_UTIL_H_
#define SRC_BASE_FILE_UTIL_H_

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "base/glog_wrapper.h"

namespace openmldb {
namespace base {

static constexpr uint32_t BOLCK_SIZE = 4 * 1024;

inline static bool Mkdir(const std::string& path) {
    if ("/" == path) {
        return true;
    }
    const int dir_mode = 0777;
    int ret = ::mkdir(path.c_str(), dir_mode);
    if (ret == 0 || errno == EEXIST) {
        return true;
    }
    PDLOG(WARNING, "mkdir %s failed err[%d: %s]", path.c_str(), errno, strerror(errno));
    return false;
}

inline static bool IsExists(const std::string& path) {
    struct stat buf;
    int ret = ::lstat(path.c_str(), &buf);
    if (ret < 0) {
        return false;
    }
    return true;
}

inline static bool Rename(const std::string& source, const std::string& target) {
    int ret = ::rename(source.c_str(), target.c_str());
    if (ret != 0) {
        PDLOG(WARNING, "fail to rename %s to %s with error %s", source.c_str(), target.c_str(), strerror(errno));
        return false;
    }
    return true;
}

inline static bool MkdirRecur(const std::string& dir_path) {
    size_t beg = 0;
    size_t seg = dir_path.find('/', beg);
    while (seg != std::string::npos) {
        if (seg + 1 >= dir_path.size()) {
            break;
        }
        if (!Mkdir(dir_path.substr(0, seg + 1))) {
            return false;
        }
        beg = seg + 1;
        seg = dir_path.find('/', beg);
    }
    return Mkdir(dir_path);
}

inline static int GetSubDir(const std::string& path,
                            std::vector<std::string>& sub_dir) {  // NOLINT
    if (path.empty()) {
        return -1;
    }
    DIR* dir = opendir(path.c_str());
    if (dir == NULL) {
        return -1;
    }
    struct dirent* ptr;
    while ((ptr = readdir(dir)) != NULL) {
        if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0) {
            continue;
        } else if (ptr->d_type == DT_DIR) {
            sub_dir.push_back(ptr->d_name);
        }
    }
    closedir(dir);
    return 0;
}

inline static int GetSubFiles(const std::string& path, std::vector<std::string>& sub_dir) {  // NOLINT
    if (path.empty()) {
        return -1;
    }
    DIR* dir = opendir(path.c_str());
    if (dir == NULL) {
        return -1;
    }
    struct dirent* ptr;
    while ((ptr = readdir(dir)) != NULL) {
        if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0) {
            continue;
        } else if (ptr->d_type == DT_REG) {
            sub_dir.push_back(ptr->d_name);
        }
    }
    closedir(dir);
    return 0;
}

inline static int GetFileName(const std::string& path,
                              std::vector<std::string>& file_vec) {  // NOLINT
    if (path.empty()) {
        PDLOG(WARNING, "input path is empty");
        return -1;
    }
    DIR* dir = opendir(path.c_str());
    if (dir == NULL) {
        PDLOG(WARNING, "fail to open path %s for %s", path.c_str(), strerror(errno));
        return -1;
    }
    struct dirent* ptr;
    struct stat stat_buf;
    while ((ptr = readdir(dir)) != NULL) {
        if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0) {
            continue;
        }
        std::string file_path = path + "/" + ptr->d_name;
        int ret = lstat(file_path.c_str(), &stat_buf);
        if (ret == -1) {
            PDLOG(WARNING, "stat path %s failed err[%d: %s]", file_path.c_str(), errno, strerror(errno));
            closedir(dir);
            return -1;
        }
        if (S_ISREG(stat_buf.st_mode)) {
            file_vec.push_back(file_path);
        }
    }
    closedir(dir);
    return 0;
}

inline static bool GetFileSize(const std::string& file_path,
                               uint64_t& size) {  // NOLINT
    if (file_path.empty()) {
        PDLOG(WARNING, "input path is empty");
        return false;
    }
    struct stat stat_buf;
    if (lstat(file_path.c_str(), &stat_buf) < 0) {
        PDLOG(WARNING, "stat path %s failed err[%d: %s]", file_path.c_str(), errno, strerror(errno));
        return false;
    }
    if (S_ISREG(stat_buf.st_mode)) {
        size = stat_buf.st_size;
        return true;
    }
    PDLOG(WARNING, "[%s] is not a regular file", file_path.c_str());
    return false;
}

inline static bool RemoveDir(const std::string& path) {
    std::vector<std::string> file_vec;
    if (GetFileName(path, file_vec) < 0) {
        return false;
    }
    for (auto file : file_vec) {
        if (remove(file.c_str()) != 0) {
            return false;
        }
    }
    if (remove(path.c_str()) != 0) {
        return false;
    }
    return true;
}

inline static int GetChildFileName(const std::string& path,
                                   std::vector<std::string>& file_vec) {  // NOLINT
    if (path.empty()) {
        return -1;
    }
    DIR* dir = opendir(path.c_str());
    if (dir == NULL) {
        return -1;
    }
    struct dirent* ptr;
    struct stat stat_buf;
    while ((ptr = readdir(dir)) != NULL) {
        if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0) {
            continue;
        }
        std::string file_path = path + "/" + ptr->d_name;
        int ret = lstat(file_path.c_str(), &stat_buf);
        if (ret == -1) {
            closedir(dir);
            return -1;
        }
        file_vec.push_back(file_path);
    }
    closedir(dir);
    return 0;
}

inline static bool IsFolder(const std::string& path) {
    struct stat s;
    return stat(path.c_str(), &s) == 0 && (s.st_mode & S_IFDIR);
}

inline static bool RemoveDirRecursive(const std::string& path) {
    std::vector<std::string> file_vec;
    if (GetChildFileName(path, file_vec) != 0) {
        return false;
    }
    for (auto file : file_vec) {
        if (IsFolder(file)) {
            if (!RemoveDirRecursive(file)) {
                return false;
            }
        } else if (remove(file.c_str()) != 0) {
            return false;
        }
    }
    return rmdir(path.c_str()) == 0;
}

inline static std::string ParseFileNameFromPath(const std::string& path) {
    size_t index = path.rfind('/');
    if (index == std::string::npos) {
        index = 0;
    } else {
        index += 1;
    }
    return path.substr(index, path.length() - index);
}

inline static std::string ParseParentDirFromPath(const std::string& path) {
    std::string full_path = path;
    if (full_path.size() > 1 && full_path.back() == '/') {
        full_path.pop_back();
    }
    size_t index = full_path.rfind('/');
    if (index == std::string::npos) {
        return "./";
    }
    return full_path.substr(0, index + 1);
}

static bool GetDirSizeRecur(const std::string& path,
                            uint64_t& size) {  // NOLINT
    std::vector<std::string> file_vec;
    if (GetChildFileName(path, file_vec) < 0) {
        return false;
    }
    for (const auto& file : file_vec) {
        struct stat stat_buf;
        if (lstat(file.c_str(), &stat_buf) < 0) {
            PDLOG(WARNING, "stat path %s failed err[%d: %s]", path.c_str(), errno, strerror(errno));
            return false;
        }
        if (IsFolder(file)) {
            size += stat_buf.st_size;
            if (!GetDirSizeRecur(file, size)) {
                return false;
            }
        } else {
            size += stat_buf.st_size;
        }
    }
    return true;
}

__attribute__((unused)) static bool CopyFile(const std::string& src_file, const std::string& desc_file) {
    if (!IsExists(src_file)) {
        return false;
    }
    FILE* f_src = fopen(src_file.c_str(), "r");
    if (!f_src) {
        return false;
    }
    FILE* f_desc = fopen(desc_file.c_str(), "w+");
    if (!f_desc) {
        fclose(f_src);
        return false;
    }
    char buf[BOLCK_SIZE];
    bool has_error = false;
    while (!feof(f_src)) {
        size_t r_len = fread(buf, sizeof(char), BOLCK_SIZE, f_src);
        if (r_len < BOLCK_SIZE) {
            if (!feof(f_src)) {
                has_error = true;
                break;
            }
        }
        size_t w_len = fwrite(buf, sizeof(char), r_len, f_desc);
        if (w_len < r_len) {
            has_error = true;
            break;
        }
    }
    fclose(f_src);
    fclose(f_desc);
    return has_error == false;
}

inline static int HardLinkDir(const std::string& src, const std::string& dest) {
    if (!IsExists(src)) {
        LOG(WARNING) << "src not exists " << src;
        return -2;
    }

    if (IsExists(dest)) {
        RemoveDirRecursive(dest);
    }

    if (!MkdirRecur(dest)) {
        LOG(WARNING) << "mkdir failed " << dest;
        return -3;
    }
    std::vector<std::string> files;
    if (GetSubFiles(src, files) != 0) {
        LOG(WARNING) << "get sub files failed " << src;
        return -4;
    }

    for (const auto& file : files) {
        int ret = link((src + "/" + file).c_str(), (dest + "/" + file).c_str());
        if (ret) {
            LOG(WARNING) << "link failed " << src << "/" << file << " " << dest << "/" << file;
            return ret;
        }
    }
    return 0;
}

// list of paths of all files match the pattern `path`
// do not include any subfolder
inline static std::vector<std::string> FindFiles(const std::string& path) {
    std::string pattern;
    const std::string file_prefix = "file://";
    if (absl::StartsWith(path, file_prefix)) {
        pattern = path.substr(file_prefix.size());
    } else {
        pattern = path;
    }

    if (IsFolder(pattern)) {
        pattern = absl::StrCat(pattern, "/*");
    }
    glob_t glob_result;
    memset(&glob_result, 0, sizeof(glob_result));

    int return_value = glob(pattern.c_str(), GLOB_TILDE, NULL, &glob_result);
    if (return_value != 0) {
        globfree(&glob_result);
        return {};
    }

    std::vector<std::string> filenames;
    for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
        std::string filename = glob_result.gl_pathv[i];
        if (!IsFolder(filename)) {
            filenames.push_back(filename);
        }
    }

    // cleanup
    globfree(&glob_result);

    // done
    return filenames;
}

}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_FILE_UTIL_H_
