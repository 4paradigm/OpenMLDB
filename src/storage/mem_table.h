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

#ifndef SRC_STORAGE_MEM_TABLE_H_
#define SRC_STORAGE_MEM_TABLE_H_

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "proto/tablet.pb.h"
#include "base/hash.h"
#include "storage/iterator.h"
#include "storage/segment.h"
#include "storage/table.h"
#include "storage/ticket.h"
#include "base/concurrentlist.h"
#include "vm/catalog.h"

DECLARE_uint32(max_traverse_cnt);

using ::openmldb::api::LogEntry;
using ::openmldb::base::Slice;

namespace openmldb {
namespace storage {

static const uint32_t SEED = 0xe17a1465;

typedef google::protobuf::RepeatedPtrField<::openmldb::api::Dimension> Dimensions;

class MemTableTraverseIterator : public TraverseIterator {
public:
   MemTableTraverseIterator(Segment** segments, uint32_t seg_cnt, ::openmldb::storage::TTLType ttl_type,
                            uint64_t expire_time, uint64_t expire_cnt, uint32_t ts_index)
       : segments_(segments),
         seg_cnt_(seg_cnt),
         seg_idx_(0),
         pk_it_(NULL),
         it_(NULL),
         record_idx_(0),
         ts_idx_(0),
         expire_value_(expire_time, expire_cnt, ttl_type),
         ticket_(),
         traverse_cnt_(0) {
       uint32_t idx = 0;
       if (segments_[0]->GetTsIdx(ts_index, idx) == 0) {
           ts_idx_ = idx;  // 将获取到的真实的ts索引 赋值为ts_idx_
       }
   }

   ~MemTableTraverseIterator() {
       if (pk_it_ != NULL) delete pk_it_;
       if (it_ != NULL) delete it_;
   }

   inline bool Valid() {
       return pk_it_ != NULL && pk_it_->Valid() && it_ != NULL && it_->Valid() &&
              !expire_value_.IsExpired(it_->GetKey(), record_idx_);
   }

   // 二级结构next
   void Next() {
       it_->Next();
       record_idx_++;  // 记录的索引增加
       traverse_cnt_++;  // 遍历的记录数增加
       if (!it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_)) { // it_->GetKey 就是时间 record_idx_ 节点计数
           NextPK();  // 如果当前二层结构过期了 则第一层跳表指向next
           return;
       }
   }

   void NextPK() {
       delete it_;  // 此时释放之前的迭代器指针
       it_ = NULL;
       do {
           ticket_.Pop();
           if (pk_it_->Valid()) {  // 只要当前节点不为NULL 就是有效的
               pk_it_->Next(); // pk_it 指向下一个节点
           }
           if (!pk_it_->Valid()) { // 如果此时pk_it 为空 则指向下一个segment
               delete pk_it_;   // 释放pk_it_
               pk_it_ = NULL;
               seg_idx_++;   // seg_索引加1 指向下一个segment
               if (seg_idx_ < seg_cnt_) {  // 如果当前索引小于 segment数 则继续遍历后面的segment
                   pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();  // KeyEntries* entries_;
                   pk_it_->SeekToFirst();  // 主键指向
                   if (!pk_it_->Valid()) {
                       continue;
                   }
               } else {
                   break;
               }
           }
           if (it_ != NULL) {
               delete it_;
               it_ = NULL;
           }
           if (segments_[seg_idx_]->GetTsCnt() > 1) {
               KeyEntry* entry = NULL;
               if (segments_[seg_idx_]->IsSkipList(0)) {
                   entry = ((SkipListKeyEntry**)pk_it_->GetValue())[0];
               } else {
                   entry = ((ListKeyEntry**)pk_it_->GetValue())[0];
               }
               it_ = entry->entries.NewIterator();
               ticket_.Push(entry);  // ticket_ 保存的是KeyEntry指针 用于引用计数
           } else {
               if (segments_[seg_idx_]->IsSkipList()) {
                   it_ = ((SkipListKeyEntry*)pk_it_->GetValue())
                             ->entries.NewIterator();
                   ticket_.Push((SkipListKeyEntry*)pk_it_->GetValue());
               } else {
                   it_ = ((ListKeyEntry*)pk_it_->GetValue())
                             ->entries.NewIterator();
                   ticket_.Push((ListKeyEntry*)pk_it_->GetValue());
               }
           }
           it_->SeekToFirst();
           record_idx_ = 1;  // 记录第二层结构的索引  每次遍历不同的二级结构 都要重置
           traverse_cnt_++;  // 保存所有遍历过的记录数
           if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {  // 如果遍历的节点数 超过了 给定的阈值 则跳出遍历
               break;
           }
       } while (it_ == NULL || !it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_));
   }

   void Seek(const std::string& key, uint64_t ts) {
       if (pk_it_ != NULL) {  // seek 前需要将指针指向的内存释放掉，不然就造成了内存泄漏
           delete pk_it_;
           pk_it_ = NULL;
       }
       if (it_ != NULL) {
           delete it_;
           it_ = NULL;
       }
       ticket_.Pop();  // 引用计数减1
       if (seg_cnt_ > 1) {
           seg_idx_ = ::openmldb::base::hash(key.c_str(), key.length(), SEED) % seg_cnt_;
       }
       Slice spk(key);
       pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
       pk_it_->Seek(spk);  // 直接指向key 为 spk的节点
       if (pk_it_->Valid()) {
           if (segments_[seg_idx_]->GetTsCnt() > 1) {
               if (segments_[seg_idx_]->IsSkipList(ts_idx_)) {
                   SkipListKeyEntry* entry = ((SkipListKeyEntry**)pk_it_->GetValue())[ts_idx_];
                   ticket_.Push(entry);
                   it_ = entry->entries.NewIterator();
               } else {
                   ListKeyEntry* entry = ((ListKeyEntry**)pk_it_->GetValue())[ts_idx_];
                   ticket_.Push(entry);
                   it_ = entry->entries.NewIterator();
               }
           } else {
               if (segments_[seg_idx_]->IsSkipList()) {
                   ticket_.Push((SkipListKeyEntry*)pk_it_->GetValue());  // NOLINT
                   it_ = ((SkipListKeyEntry*)pk_it_->GetValue())         // NOLINT
                             ->entries.NewIterator();
               } else {
                   ticket_.Push((ListKeyEntry*)pk_it_->GetValue());  // NOLINT
                   it_ = ((ListKeyEntry*)pk_it_->GetValue())         // NOLINT
                             ->entries.NewIterator();
               }
           }
           if (spk.compare(pk_it_->GetKey()) != 0 || ts == 0) {
               it_->SeekToFirst();
               traverse_cnt_++;
               record_idx_ = 1;
               if (!it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
                   NextPK();
               }
           } else {
               if (expire_value_.ttl_type == ::openmldb::storage::TTLType::kLatestTime) {
                   it_->SeekToFirst();
                   record_idx_ = 1;
                   while (it_->Valid() && record_idx_ <= expire_value_.lat_ttl) {
                       traverse_cnt_++;
                       if (it_->GetKey() < ts) {
                           return;
                       }
                       it_->Next();
                       record_idx_++;
                   }
                   NextPK();
               } else {
                   it_->Seek(ts);
                   traverse_cnt_++;
                   if (it_->Valid() && it_->GetKey() == ts) {
                       it_->Next();
                   }
                   if (!it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
                       NextPK();
                   }
               }
           }
       } else {
           NextPK();
       }
   }

   openmldb::base::Slice GetValue() const {
       return openmldb::base::Slice(it_->GetValue()->data, it_->GetValue()->size);
   }

   std::string GetPK() const {
       if (pk_it_ == NULL) {
           return std::string();
       }
       return pk_it_->GetKey().ToString();
   }

   uint64_t GetKey() {
       if (it_ != NULL && it_->Valid()) {
           return it_->GetKey();
       }
       return UINT64_MAX;
   }
   void SeekToFirst() {
       ticket_.Pop();
       if (pk_it_ != NULL) {
           delete pk_it_;
           pk_it_ = NULL;
       }
       if (it_ != NULL) {
           delete it_;
           it_ = NULL;
       }
       for (seg_idx_ = 0; seg_idx_ < seg_cnt_; seg_idx_++) {
           pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
           pk_it_->SeekToFirst();
           while (pk_it_->Valid()) {
               if (segments_[seg_idx_]->GetTsCnt() > 1) {
                   KeyEntry* entry = NULL;
                   if (segments_[seg_idx_]->IsSkipList(ts_idx_)) {
                       entry = ((SkipListKeyEntry**)pk_it_->GetValue())[ts_idx_];
                   } else {
                       entry = ((ListKeyEntry**)pk_it_->GetValue())[ts_idx_];
                   }
                   ticket_.Push(entry);
                   it_ = entry->entries.NewIterator();
               } else {
                   if (segments_[seg_idx_]->IsSkipList()) {
                       ticket_.Push((SkipListKeyEntry*)pk_it_->GetValue());  // NOLINT
                       it_ = ((SkipListKeyEntry*)pk_it_->GetValue())         // NOLINT
                                 ->entries.NewIterator();
                   } else {
                       ticket_.Push((ListKeyEntry*)pk_it_->GetValue());  // NOLINT
                       it_ = ((ListKeyEntry*)pk_it_->GetValue())         // NOLINT
                                 ->entries.NewIterator();
                   }
               }
               it_->SeekToFirst();
               traverse_cnt_++;
               if (it_->Valid() && !expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
                   record_idx_ = 1;
                   return;
               }
               delete it_; // 如果it 无效或过期 释放内存
               it_ = NULL;
               pk_it_->Next();
               ticket_.Pop(); // pop
               if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
                   return;
               }
           }
           delete pk_it_;
           pk_it_ = NULL;
       }
   }
   uint64_t GetCount() const { return traverse_cnt_; }  // 返回遍历的所有节点

private:
   Segment** segments_;      // segment指针数组
   uint32_t const seg_cnt_;  // segment指针数组长度
   uint32_t seg_idx_;        // segment指针数组当前的索引
   KeyEntries::Iterator* pk_it_;  // 第一层跳表迭代器
   BaseTimeEntriesIterator* it_;  // 第二层结构的迭代器
   uint32_t record_idx_;  // 遍历的记录数
   uint32_t ts_idx_;      // ts index
   // uint64_t expire_value_;
   TTLSt expire_value_;  // 过期值
   Ticket ticket_;  // 保存it_ 用来进行引用计数
   uint64_t traverse_cnt_;  // 记录遍历的所有节点
};

class MemTable : public Table {
public:
   MemTable(const std::string& name, uint32_t id, uint32_t pid, uint32_t seg_cnt,
            const std::map<std::string, uint32_t>& mapping, uint64_t ttl, ::openmldb::type::TTLType ttl_type);

   explicit MemTable(const ::openmldb::api::TableMeta& table_meta);
   virtual ~MemTable();
   MemTable(const MemTable&) = delete;
   MemTable& operator=(const MemTable&) = delete;

   bool Init() override;

   bool Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) override;

   bool Put(uint64_t time, const std::string& value, const Dimensions& dimensions) override;

   bool GetBulkLoadInfo(::openmldb::api::BulkLoadInfoResponse* response);

   bool BulkLoad(const std::vector<DataBlock*>& data_blocks,
                 const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes);

   bool Delete(const std::string& pk, uint32_t idx) override;

   // use the first demission
   TableIterator* NewIterator(const std::string& pk, Ticket& ticket) override;

   TableIterator* NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) override;

   TraverseIterator* NewTraverseIterator(uint32_t index) override;

   ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t index);

   // release all memory allocated
   uint64_t Release();

   void SchedGc() override;

   int GetCount(uint32_t index, const std::string& pk, uint64_t& count) override;  // NOLINT

   uint64_t GetRecordIdxCnt() override;
   bool GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size) override;
   uint64_t GetRecordIdxByteSize() override;
   uint64_t GetRecordPkCnt() override;

   void SetCompressType(::openmldb::type::CompressType compress_type);
   ::openmldb::type::CompressType GetCompressType();

   uint64_t GetRecordByteSize() const override { return record_byte_size_.load(std::memory_order_relaxed); }

   uint64_t GetRecordCnt() const override { return record_cnt_.load(std::memory_order_relaxed); }

   inline uint32_t GetSegCnt() const { return seg_cnt_; }

   inline void SetExpire(bool is_expire) { enable_gc_.store(is_expire, std::memory_order_relaxed); }

   uint64_t GetExpireTime(const TTLSt& ttl_st) override;

   bool IsExpire(const ::openmldb::api::LogEntry& entry) override;

   inline bool GetExpireStatus() { return enable_gc_.load(std::memory_order_relaxed); }

   inline void RecordCntIncr() { record_cnt_.fetch_add(1, std::memory_order_relaxed); }

   inline void RecordCntIncr(uint32_t cnt) { record_cnt_.fetch_add(cnt, std::memory_order_relaxed); }

   inline uint32_t GetKeyEntryHeight() const { return key_entry_max_height_; }

   bool DeleteIndex(const std::string& idx_name) override;

   bool AddIndex(const ::openmldb::common::ColumnKey& column_key);

private:
   bool CheckAbsolute(const TTLSt& ttl, uint64_t ts);

   bool CheckLatest(uint32_t index_id, const std::string& key, uint64_t ts);

private:
   uint32_t seg_cnt_;
   std::vector<Segment**> segments_;
   std::atomic<bool> enable_gc_;
   uint64_t ttl_offset_;
   std::atomic<uint64_t> record_cnt_;
   bool segment_released_;
   std::atomic<uint64_t> record_byte_size_;
   uint32_t key_entry_max_height_;
};

}  // namespace storage
}  // namespace openmldb

#endif  // SRC_STORAGE_MEM_TABLE_H_
