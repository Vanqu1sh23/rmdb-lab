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

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<std::pair<ColMeta, bool>> order_cols_;
    int limit_;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    size_t cursor_ = 0;
    bool initialized_ = false;

    void load_and_sort() {
        if (initialized_) return;
        initialized_ = true;
        tuples_.clear();
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            tuples_.push_back(prev_->Next());
        }
        if (!order_cols_.empty()) {
            std::stable_sort(tuples_.begin(), tuples_.end(), [&](const auto &lhs, const auto &rhs) {
                for (auto &order_col : order_cols_) {
                    const auto &col = order_col.first;
                    bool is_desc = order_col.second;
                    int cmp = compare_raw(lhs->data + col.offset, rhs->data + col.offset, col.type, col.type, col.len);
                    if (cmp != 0) {
                        return is_desc ? cmp > 0 : cmp < 0;
                    }
                }
                return false;
            });
        }
        if (limit_ >= 0 && static_cast<size_t>(limit_) < tuples_.size()) {
            tuples_.resize(static_cast<size_t>(limit_));
        }
    }

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<std::pair<TabCol, bool>> &order_cols, int limit) {
        prev_ = std::move(prev);
        for (auto &order_col : order_cols) {
            order_cols_.emplace_back(prev_->get_col_offset(order_col.first), order_col.second);
        }
        limit_ = limit;
    }

    size_t tupleLen() const override { return prev_->tupleLen(); }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    void beginTuple() override {
        load_and_sort();
        cursor_ = 0;
    }

    void nextTuple() override {
        if (!is_end()) {
            cursor_++;
        }
    }

    bool is_end() const override { return cursor_ >= tuples_.size(); }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*tuples_[cursor_]);
    }

    Rid &rid() override { return _abstract_rid; }
};
