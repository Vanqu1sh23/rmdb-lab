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
#include <limits>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::vector<Rid> matched_rids_;
    size_t cursor_ = 0;

    SmManager *sm_manager_;

    void fill_min(char *dest, const ColMeta &col) {
        if (col.type == TYPE_INT) { int v = std::numeric_limits<int>::min(); memcpy(dest, &v, sizeof(v)); }
        else if (col.type == TYPE_BIGINT || col.type == TYPE_DATETIME) { int64_t v = std::numeric_limits<int64_t>::min(); memcpy(dest, &v, sizeof(v)); }
        else if (col.type == TYPE_FLOAT) { float v = -std::numeric_limits<float>::infinity(); memcpy(dest, &v, sizeof(v)); }
        else { memset(dest, 0, col.len); }
    }

    void fill_max(char *dest, const ColMeta &col) {
        if (col.type == TYPE_INT) { int v = std::numeric_limits<int>::max(); memcpy(dest, &v, sizeof(v)); }
        else if (col.type == TYPE_BIGINT || col.type == TYPE_DATETIME) { int64_t v = std::numeric_limits<int64_t>::max(); memcpy(dest, &v, sizeof(v)); }
        else if (col.type == TYPE_FLOAT) { float v = std::numeric_limits<float>::infinity(); memcpy(dest, &v, sizeof(v)); }
        else { memset(dest, 0xff, col.len); }
    }

    std::vector<Condition> column_conds(const std::string &col_name) const {
        std::vector<Condition> out;
        for (auto &cond : fed_conds_) {
            if (cond.is_rhs_val && cond.lhs_col.tab_name == tab_name_ && cond.lhs_col.col_name == col_name) {
                out.push_back(cond);
            }
        }
        return out;
    }

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
            if (!context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd())) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        matched_rids_.clear();
        cursor_ = 0;
        std::vector<char> lower(index_meta_.col_tot_len);
        std::vector<char> upper(index_meta_.col_tot_len);
        int offset = 0;
        bool lower_inc = true, upper_inc = true;
        bool stopped = false;
        for (auto &col : index_meta_.cols) {
            if (stopped) {
                fill_min(lower.data() + offset, col);
                fill_max(upper.data() + offset, col);
                offset += col.len;
                continue;
            }
            auto conds = column_conds(col.name);
            Condition *eq = nullptr;
            Condition *lower_cond = nullptr;
            Condition *upper_cond = nullptr;
            for (auto &cond : conds) {
                if (cond.op == OP_EQ) eq = &cond;
                else if (cond.op == OP_GT || cond.op == OP_GE) lower_cond = &cond;
                else if (cond.op == OP_LT || cond.op == OP_LE) upper_cond = &cond;
            }
            if (eq != nullptr) {
                memcpy(lower.data() + offset, eq->rhs_val.raw->data, col.len);
                memcpy(upper.data() + offset, eq->rhs_val.raw->data, col.len);
            } else {
                if (lower_cond != nullptr) {
                    memcpy(lower.data() + offset, lower_cond->rhs_val.raw->data, col.len);
                    lower_inc = lower_cond->op == OP_GE;
                } else {
                    fill_min(lower.data() + offset, col);
                }
                if (upper_cond != nullptr) {
                    memcpy(upper.data() + offset, upper_cond->rhs_val.raw->data, col.len);
                    upper_inc = upper_cond->op == OP_LE;
                } else {
                    fill_max(upper.data() + offset, col);
                }
                stopped = true;
            }
            offset += col.len;
        }
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
        auto candidates = ih->range_scan(lower.data(), true, lower_inc, upper.data(), true, upper_inc);
        for (auto &candidate : candidates) {
            auto rec = fh_->get_record(candidate, context_);
            if (eval_conds(fed_conds_, cols_, rec.get())) {
                matched_rids_.push_back(candidate);
            }
        }
        if (!matched_rids_.empty()) rid_ = matched_rids_[0];
        else rid_ = {RM_NO_PAGE, -1};
    }

    void nextTuple() override {
        if (cursor_ < matched_rids_.size()) cursor_++;
        if (cursor_ < matched_rids_.size()) rid_ = matched_rids_[cursor_];
        else rid_ = {RM_NO_PAGE, -1};
    }

    bool is_end() const override { return cursor_ >= matched_rids_.size(); }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return rid_; }
};