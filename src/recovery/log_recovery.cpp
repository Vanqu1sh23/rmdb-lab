/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

#include <fcntl.h>
#include <unistd.h>

#include <memory>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "record/rm_scan.h"

namespace {
std::vector<std::unique_ptr<LogRecord>> logs;
std::unordered_set<txn_id_t> finished_txns;
std::unordered_set<txn_id_t> active_txns;
std::unordered_set<std::string> touched_tables;

std::unique_ptr<LogRecord> parse_log_record(const char *data) {
    LogType type = *reinterpret_cast<const LogType *>(data + OFFSET_LOG_TYPE);
    std::unique_ptr<LogRecord> rec;
    if (type == LogType::begin) rec = std::make_unique<BeginLogRecord>();
    else if (type == LogType::commit) rec = std::make_unique<CommitLogRecord>();
    else if (type == LogType::ABORT) rec = std::make_unique<AbortLogRecord>();
    else rec = std::make_unique<RecordLogRecord>();
    rec->deserialize(data);
    return rec;
}

void apply_insert(SmManager *sm_manager, const RecordLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    try {
        fh->insert_record(log.rid_, log.new_value_.data);
    } catch (RMDBError &) {
        fh->update_record(log.rid_, log.new_value_.data, nullptr);
    }
}

void apply_delete(SmManager *sm_manager, const RecordLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    try {
        fh->delete_record(log.rid_, nullptr);
    } catch (RMDBError &) {
    }
}

void apply_update(SmManager *sm_manager, const RecordLogRecord &log, bool use_new_value) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    auto &rec = use_new_value ? log.new_value_ : log.old_value_;
    try {
        fh->update_record(log.rid_, rec.data, nullptr);
    } catch (RMDBError &) {
        fh->insert_record(log.rid_, rec.data);
    }
}

std::vector<char> make_index_key(const IndexMeta &index, const RmRecord &rec) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (auto &col : index.cols) {
        memcpy(key.data() + offset, rec.data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}

void rebuild_table_indexes(SmManager *sm_manager, const std::string &tab_name) {
    if (!sm_manager->db_.is_table(tab_name)) return;
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
 * @description: analyze阶段，获得事务状态
 */
void RecoveryManager::analyze() {
    logs.clear();
    finished_txns.clear();
    active_txns.clear();
    touched_tables.clear();
    if (!disk_manager_->is_file(LOG_FILE_NAME)) return;

    int offset = 0;
    while (true) {
        char header[LOG_HEADER_SIZE];
        int n = disk_manager_->read_log(header, LOG_HEADER_SIZE, offset);
        if (n <= 0) break;
        if (n < LOG_HEADER_SIZE) break;
        uint32_t len = *reinterpret_cast<uint32_t *>(header + OFFSET_LOG_TOT_LEN);
        if (len < LOG_HEADER_SIZE || len > LOG_BUFFER_SIZE) break;
        std::vector<char> data(len);
        n = disk_manager_->read_log(data.data(), len, offset);
        if (n != static_cast<int>(len)) break;
        auto rec = parse_log_record(data.data());
        if (rec->log_type_ == LogType::begin) {
            active_txns.insert(rec->log_tid_);
        } else if (rec->log_type_ == LogType::commit || rec->log_type_ == LogType::ABORT) {
            finished_txns.insert(rec->log_tid_);
            active_txns.erase(rec->log_tid_);
        } else {
            active_txns.insert(rec->log_tid_);
            auto *record_log = dynamic_cast<RecordLogRecord *>(rec.get());
            if (record_log != nullptr) touched_tables.insert(record_log->table_name_);
        }
        logs.push_back(std::move(rec));
        offset += len;
    }
}

/**
 * @description: 重做所有已提交事务
 */
void RecoveryManager::redo() {
    for (auto &rec : logs) {
        if (!finished_txns.count(rec->log_tid_)) continue;
        auto *record_log = dynamic_cast<RecordLogRecord *>(rec.get());
        if (record_log == nullptr) continue;
        if (rec->log_type_ == LogType::INSERT) apply_insert(sm_manager_, *record_log);
        else if (rec->log_type_ == LogType::DELETE) apply_delete(sm_manager_, *record_log);
        else if (rec->log_type_ == LogType::UPDATE) apply_update(sm_manager_, *record_log, true);
    }
}

/**
 * @description: 回滚未完成事务
 */
void RecoveryManager::undo() {
    for (auto it = logs.rbegin(); it != logs.rend(); ++it) {
        auto &rec = *it;
        if (!active_txns.count(rec->log_tid_)) continue;
        auto *record_log = dynamic_cast<RecordLogRecord *>(rec.get());
        if (record_log == nullptr) continue;
        if (rec->log_type_ == LogType::INSERT) apply_delete(sm_manager_, *record_log);
        else if (rec->log_type_ == LogType::DELETE) apply_insert(sm_manager_, *record_log);
        else if (rec->log_type_ == LogType::UPDATE) apply_update(sm_manager_, *record_log, false);
    }
    for (auto &tab_name : touched_tables) rebuild_table_indexes(sm_manager_, tab_name);
    for (auto &entry : sm_manager_->fhs_) {
        sm_manager_->get_bpm()->flush_all_pages(entry.second->GetFd());
    }
    int fd = open(LOG_FILE_NAME.c_str(), O_WRONLY | O_TRUNC, 0644);
    if (fd >= 0) close(fd);
}
