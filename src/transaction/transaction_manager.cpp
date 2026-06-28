/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "record/rm_scan.h"
#include "system/sm_manager.h"

#include <vector>
#include <unordered_set>

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

namespace {
std::vector<char> make_index_key(const IndexMeta &index, const RmRecord &rec) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (auto &col : index.cols) {
        memcpy(key.data() + offset, rec.data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}

void delete_index_entries(SmManager *sm_manager, const std::string &tab_name, const TabMeta &tab, const RmRecord &rec) {
    for (auto &index : tab.indexes) {
        auto key = make_index_key(index, rec);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->delete_entry(key.data(), nullptr);
    }
}

void insert_index_entries(SmManager *sm_manager, const std::string &tab_name, const TabMeta &tab, const RmRecord &rec, const Rid &rid) {
    for (auto &index : tab.indexes) {
        auto key = make_index_key(index, rec);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->insert_entry(key.data(), rid, nullptr);
    }
}

void rebuild_table_indexes(SmManager *sm_manager, const std::string &tab_name) {
    auto tab = sm_manager->db_.get_table(tab_name);
    if (tab.indexes.empty()) return;
    auto fh = sm_manager->fhs_.at(tab_name).get();
    for (auto &index : tab.indexes) {
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->clear_entries();
    }
    for (RmScan scan(fh); !scan.is_end(); scan.next()) {
        auto rec = fh->get_record(scan.rid(), nullptr);
        for (auto &index : tab.indexes) {
            auto key = make_index_key(index, *rec);
            auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
            ih->insert_entry(key.data(), scan.rid(), nullptr);
        }
    }
}
}

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
    }
    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_++);
    std::unique_lock<std::mutex> lock(latch_);
    txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) return;
    if (txn->get_write_set() != nullptr) {
        for (auto *wr : *txn->get_write_set()) {
            delete wr;
        }
        txn->get_write_set()->clear();
    }
    if (txn->get_lock_set() != nullptr) {
        auto locks = *txn->get_lock_set();
        for (auto &lock_data_id : locks) {
            lock_manager_->unlock(txn, lock_data_id);
        }
        txn->get_lock_set()->clear();
    }
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    if (txn == nullptr) return;
    auto write_set = txn->get_write_set();
    std::unordered_set<std::string> touched_tables;
    if (write_set != nullptr) {
        for (auto *wr : *write_set) {
            touched_tables.insert(wr->GetTableName());
        }
        for (auto it = write_set->rbegin(); it != write_set->rend(); ++it) {
            WriteRecord *wr = *it;
            const auto &tab_name = wr->GetTableName();
            auto fh = sm_manager_->fhs_.at(tab_name).get();
            auto tab = sm_manager_->db_.get_table(tab_name);
            if (wr->GetWriteType() == WType::INSERT_TUPLE) {
                auto inserted_rec = fh->get_record(wr->GetRid(), nullptr);
                delete_index_entries(sm_manager_, tab_name, tab, *inserted_rec);
                fh->delete_record(wr->GetRid(), nullptr);
            } else if (wr->GetWriteType() == WType::DELETE_TUPLE) {
                fh->insert_record(wr->GetRid(), wr->GetRecord().data);
                insert_index_entries(sm_manager_, tab_name, tab, wr->GetRecord(), wr->GetRid());
            } else if (wr->GetWriteType() == WType::UPDATE_TUPLE) {
                delete_index_entries(sm_manager_, tab_name, tab, wr->GetNewRecord());
                fh->update_record(wr->GetRid(), wr->GetRecord().data, nullptr);
                insert_index_entries(sm_manager_, tab_name, tab, wr->GetRecord(), wr->GetRid());
            }
            delete wr;
        }
        write_set->clear();
    }
    for (auto &tab_name : touched_tables) {
        rebuild_table_indexes(sm_manager_, tab_name);
    }
    if (txn->get_lock_set() != nullptr) {
        auto locks = *txn->get_lock_set();
        for (auto &lock_data_id : locks) {
            lock_manager_->unlock(txn, lock_data_id);
        }
        txn->get_lock_set()->clear();
    }
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
    txn->set_state(TransactionState::ABORTED);
}
