/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

namespace {
bool is_table_lock(const LockDataId &lock_data_id) {
    return lock_data_id.type_ == LockDataType::TABLE;
}
}

bool LockManager::compatible(LockMode held, LockMode requested) {
    if (held == LockMode::SHARED && requested == LockMode::SHARED) return true;
    if (held == LockMode::INTENTION_SHARED && requested == LockMode::INTENTION_SHARED) return true;
    if (held == LockMode::INTENTION_SHARED && requested == LockMode::INTENTION_EXCLUSIVE) return true;
    if (held == LockMode::INTENTION_EXCLUSIVE && requested == LockMode::INTENTION_SHARED) return true;
    if (held == LockMode::INTENTION_EXCLUSIVE && requested == LockMode::INTENTION_EXCLUSIVE) return true;
    if (held == LockMode::INTENTION_SHARED && requested == LockMode::SHARED) return true;
    if (held == LockMode::SHARED && requested == LockMode::INTENTION_SHARED) return true;
    return false;
}

LockManager::GroupLockMode LockManager::compute_group_mode(const LockRequestQueue &queue) {
    bool has_s = false, has_x = false, has_is = false, has_ix = false, has_six = false;
    for (auto &req : queue.request_queue_) {
        if (!req.granted_) continue;
        if (req.lock_mode_ == LockMode::SHARED) has_s = true;
        else if (req.lock_mode_ == LockMode::EXLUCSIVE) has_x = true;
        else if (req.lock_mode_ == LockMode::INTENTION_SHARED) has_is = true;
        else if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE) has_ix = true;
        else if (req.lock_mode_ == LockMode::S_IX) has_six = true;
    }
    if (has_x) return GroupLockMode::X;
    if (has_six || (has_s && has_ix)) return GroupLockMode::SIX;
    if (has_s) return GroupLockMode::S;
    if (has_ix) return GroupLockMode::IX;
    if (has_is) return GroupLockMode::IS;
    return GroupLockMode::NON_LOCK;
}

bool LockManager::lock(Transaction *txn, const LockDataId &lock_data_id, LockMode lock_mode) {
    if (txn == nullptr) return true;
    std::unique_lock<std::mutex> lock(latch_);
    if (txn->get_state() == TransactionState::SHRINKING || txn->get_state() == TransactionState::ABORTED) {
        return false;
    }

    auto &queue = lock_table_[lock_data_id];
    auto own = queue.request_queue_.end();
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id() && it->granted_) {
            own = it;
            break;
        }
    }

    if (own != queue.request_queue_.end()) {
        if (own->lock_mode_ == lock_mode || own->lock_mode_ == LockMode::EXLUCSIVE) return true;
        if (own->lock_mode_ == LockMode::SHARED && lock_mode == LockMode::EXLUCSIVE) {
            for (auto &req : queue.request_queue_) {
                if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
                    return false;
                }
            }
            own->lock_mode_ = LockMode::EXLUCSIVE;
            queue.group_lock_mode_ = compute_group_mode(queue);
            txn->get_lock_set()->insert(lock_data_id);
            return true;
        }
        return true;
    }

    for (auto &req : queue.request_queue_) {
        if (req.granted_ && !compatible(req.lock_mode_, lock_mode)) {
            return false;
        }
    }
    queue.request_queue_.emplace_back(txn->get_transaction_id(), lock_mode);
    queue.request_queue_.back().granted_ = true;
    queue.group_lock_mode_ = compute_group_mode(queue);
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

/**
 * @description: 申请行级共享锁
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, rid, LockDataType::RECORD), LockMode::SHARED);
}

/**
 * @description: 申请行级排他锁
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, rid, LockDataType::RECORD), LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级读锁
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::SHARED);
}

/**
 * @description: 申请表级写锁
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级意向读锁
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::INTENTION_SHARED);
}

/**
 * @description: 申请表级意向写锁
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::INTENTION_EXCLUSIVE);
}

/**
 * @description: 释放锁
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    if (txn == nullptr) return true;
    std::unique_lock<std::mutex> lock(latch_);
    auto table_it = lock_table_.find(lock_data_id);
    if (table_it == lock_table_.end()) return false;
    auto &queue = table_it->second;
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            queue.request_queue_.erase(it);
            break;
        }
    }
    queue.group_lock_mode_ = compute_group_mode(queue);
    if (queue.request_queue_.empty()) {
        lock_table_.erase(table_it);
    }
    if (txn->get_lock_set() != nullptr) {
        txn->get_lock_set()->erase(lock_data_id);
    }
    if (txn->get_state() == TransactionState::GROWING && !is_table_lock(lock_data_id)) {
        txn->set_state(TransactionState::SHRINKING);
    }
    return true;
}
