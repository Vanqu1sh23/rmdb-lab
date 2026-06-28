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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        for (auto &rid : rids_) {
            auto old_rec = fh_->get_record(rid, context_);
            RmRecord new_rec(*old_rec);
            for (auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                cast_value_to_col(set_clause.rhs, *col);
                if (set_clause.rhs.raw == nullptr) {
                    set_clause.rhs.init_raw(col->len);
                }
                memcpy(new_rec.data + col->offset, set_clause.rhs.raw->data, col->len);
            }

            struct IndexKeyPair { IndexMeta index; std::vector<char> old_key; std::vector<char> new_key; };
            std::vector<IndexKeyPair> index_keys;
            for (auto &index : tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                IndexKeyPair pair{index, std::vector<char>(index.col_tot_len), std::vector<char>(index.col_tot_len)};
                int offset = 0;
                for (auto &col : index.cols) {
                    memcpy(pair.old_key.data() + offset, old_rec->data + col.offset, col.len);
                    memcpy(pair.new_key.data() + offset, new_rec.data + col.offset, col.len);
                    offset += col.len;
                }
                std::vector<Rid> existing;
                if (memcmp(pair.old_key.data(), pair.new_key.data(), index.col_tot_len) != 0 &&
                    ih->get_value(pair.new_key.data(), &existing, context_ == nullptr ? nullptr : context_->txn_)) {
                    throw InternalError("Duplicate key for unique index");
                }
                index_keys.push_back(std::move(pair));
            }

            for (auto &pair : index_keys) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, pair.index.cols)).get();
                ih->delete_entry(pair.old_key.data(), context_ == nullptr ? nullptr : context_->txn_);
                ih->insert_entry(pair.new_key.data(), rid, context_ == nullptr ? nullptr : context_->txn_);
            }

            fh_->update_record(rid, new_rec.data, context_);
            if (context_ != nullptr && context_->txn_ != nullptr) {
                context_->txn_->append_write_record(new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *old_rec, new_rec));
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
