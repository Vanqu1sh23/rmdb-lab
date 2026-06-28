/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <memory>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    static constexpr size_t JOIN_BUFFER_SIZE = 16 * 1024 * 1024;

    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend = false;
    std::vector<std::unique_ptr<RmRecord>> left_block_;
    std::vector<std::unique_ptr<RmRecord>> right_cache_;
    std::unique_ptr<RmRecord> left_rec_;
    std::unique_ptr<RmRecord> right_rec_;
    std::unique_ptr<RmRecord> current_tuple_;
    size_t left_block_pos_ = 0;
    size_t right_cache_pos_ = 0;
    bool cached_right_mode_ = false;

    bool try_cache_right() {
        right_cache_.clear();
        size_t bytes = 0;
        const size_t right_tuple_len = right_->tupleLen();
        for (right_->beginTuple(); !right_->is_end(); right_->nextTuple()) {
            if (!right_cache_.empty() && bytes + right_tuple_len > JOIN_BUFFER_SIZE) {
                right_cache_.clear();
                right_->beginTuple();
                return false;
            }
            right_cache_.push_back(right_->Next());
            bytes += right_tuple_len;
        }
        return true;
    }

    bool load_left_block() {
        left_block_.clear();
        size_t bytes = 0;
        const size_t left_tuple_len = left_->tupleLen();
        while (!left_->is_end()) {
            if (!left_block_.empty() && bytes + left_tuple_len > JOIN_BUFFER_SIZE) {
                break;
            }
            left_block_.push_back(left_->Next());
            bytes += left_tuple_len;
            left_->nextTuple();
        }
        return !left_block_.empty();
    }

    std::unique_ptr<RmRecord> make_join_record(const RmRecord *left_rec, const RmRecord *right_rec) const {
        auto rec = std::make_unique<RmRecord>(len_);
        memcpy(rec->data, left_rec->data, left_->tupleLen());
        memcpy(rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
        return rec;
    }

    bool find_next_with_cached_right() {
        current_tuple_ = nullptr;
        while (!left_->is_end()) {
            if (left_rec_ == nullptr) {
                left_rec_ = left_->Next();
                right_cache_pos_ = 0;
            }
            while (right_cache_pos_ < right_cache_.size()) {
                auto joined = make_join_record(left_rec_.get(), right_cache_[right_cache_pos_].get());
                right_cache_pos_++;
                if (eval_conds(fed_conds_, cols_, joined.get())) {
                    current_tuple_ = std::move(joined);
                    isend = false;
                    return true;
                }
            }
            left_->nextTuple();
            left_rec_ = nullptr;
        }
        isend = true;
        return false;
    }

    bool find_next_match() {
        if (cached_right_mode_) {
            return find_next_with_cached_right();
        }
        current_tuple_ = nullptr;
        while (!left_block_.empty()) {
            while (true) {
                if (right_rec_ == nullptr) {
                    if (right_->is_end()) break;
                    right_rec_ = right_->Next();
                    left_block_pos_ = 0;
                }
                while (left_block_pos_ < left_block_.size()) {
                    auto joined = make_join_record(left_block_[left_block_pos_].get(), right_rec_.get());
                    left_block_pos_++;
                    if (eval_conds(fed_conds_, cols_, joined.get())) {
                        current_tuple_ = std::move(joined);
                        isend = false;
                        return true;
                    }
                }
                right_->nextTuple();
                right_rec_ = nullptr;
            }
            if (!load_left_block()) break;
            right_->beginTuple();
            right_rec_ = nullptr;
            left_block_pos_ = 0;
        }
        isend = true;
        return false;
    }

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);
    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
        left_rec_ = nullptr;
        right_rec_ = nullptr;
        current_tuple_ = nullptr;
        left_block_pos_ = 0;
        right_cache_pos_ = 0;
        isend = false;
        cached_right_mode_ = try_cache_right();
        if (cached_right_mode_) {
            find_next_match();
            return;
        }
        if (!load_left_block()) {
            isend = true;
            return;
        }
        find_next_match();
    }

    void nextTuple() override {
        if (isend) return;
        find_next_match();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (current_tuple_ == nullptr) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*current_tuple_);
    }

    bool is_end() const override { return isend; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return _abstract_rid; }
};
