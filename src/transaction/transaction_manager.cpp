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
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

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
    if (write_set != nullptr) {
        for (auto it = write_set->rbegin(); it != write_set->rend(); ++it) {
            WriteRecord *wr = *it;
            auto fh = sm_manager_->fhs_.at(wr->GetTableName()).get();
            if (wr->GetWriteType() == WType::INSERT_TUPLE) {
                fh->delete_record(wr->GetRid(), nullptr);
            } else if (wr->GetWriteType() == WType::DELETE_TUPLE) {
                fh->insert_record(wr->GetRid(), wr->GetRecord().data);
            } else if (wr->GetWriteType() == WType::UPDATE_TUPLE) {
                fh->update_record(wr->GetRid(), wr->GetRecord().data, nullptr);
            }
            delete wr;
        }
        write_set->clear();
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
