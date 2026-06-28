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

#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "log_defs.h"
#include "record/rm_defs.h"

/* 日志记录对应操作的类型 */
enum LogType: int {
    UPDATE = 0,
    INSERT,
    DELETE,
    begin,
    commit,
    ABORT
};
static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT"
};

class LogRecord {
public:
    LogType log_type_;
    lsn_t lsn_;
    uint32_t log_tot_len_;
    txn_id_t log_tid_;
    lsn_t prev_lsn_;

    virtual ~LogRecord() = default;

    virtual void serialize(char* dest) const {
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }

    virtual void deserialize(const char* src) {
        log_type_ = *reinterpret_cast<const LogType*>(src + OFFSET_LOG_TYPE);
        lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_LSN);
        log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = *reinterpret_cast<const txn_id_t*>(src + OFFSET_LOG_TID);
        prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_PREV_LSN);
    }

    virtual void format_print() {
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
        printf("lsn: %d\n", lsn_);
        printf("log_tot_len: %d\n", log_tot_len_);
        printf("log_tid: %d\n", log_tid_);
        printf("prev_lsn: %d\n", prev_lsn_);
    }
};

class BeginLogRecord: public LogRecord {
public:
    BeginLogRecord() {
        log_type_ = LogType::begin;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() { log_tid_ = txn_id; }
};

class CommitLogRecord: public LogRecord {
public:
    CommitLogRecord() {
        log_type_ = LogType::commit;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit CommitLogRecord(txn_id_t txn_id) : CommitLogRecord() { log_tid_ = txn_id; }
};

class AbortLogRecord: public LogRecord {
public:
    AbortLogRecord() {
        log_type_ = LogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit AbortLogRecord(txn_id_t txn_id) : AbortLogRecord() { log_tid_ = txn_id; }
};

class RecordLogRecord: public LogRecord {
public:
    RmRecord old_value_;
    RmRecord new_value_;
    Rid rid_;
    std::string table_name_;

    RecordLogRecord() = default;

    RecordLogRecord(LogType type, txn_id_t txn_id, const std::string &table_name, const Rid &rid,
                    const RmRecord *old_value, const RmRecord *new_value) {
        old_value_.size = 0;
        old_value_.data = nullptr;
        old_value_.allocated_ = false;
        new_value_.size = 0;
        new_value_.data = nullptr;
        new_value_.allocated_ = false;
        log_type_ = type;
        lsn_ = INVALID_LSN;
        log_tid_ = txn_id;
        prev_lsn_ = INVALID_LSN;
        table_name_ = table_name;
        rid_ = rid;
        if (old_value != nullptr) old_value_ = *old_value;
        if (new_value != nullptr) new_value_ = *new_value;
        log_tot_len_ = LOG_HEADER_SIZE + sizeof(Rid) + sizeof(size_t) + table_name_.size() + sizeof(int) +
                       old_value_.size + sizeof(int) + new_value_.size;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        size_t table_name_size = table_name_.size();
        memcpy(dest + offset, &table_name_size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_.data(), table_name_size);
        offset += table_name_size;
        memcpy(dest + offset, &old_value_.size, sizeof(int));
        offset += sizeof(int);
        if (old_value_.size > 0) {
            memcpy(dest + offset, old_value_.data, old_value_.size);
            offset += old_value_.size;
        }
        memcpy(dest + offset, &new_value_.size, sizeof(int));
        offset += sizeof(int);
        if (new_value_.size > 0) {
            memcpy(dest + offset, new_value_.data, new_value_.size);
        }
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        rid_ = *reinterpret_cast<const Rid *>(src + offset);
        offset += sizeof(Rid);
        size_t table_name_size = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        table_name_.assign(src + offset, table_name_size);
        offset += table_name_size;
        int old_size = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        if (old_size > 0) {
            old_value_.Deserialize(src + offset - sizeof(int));
            offset += old_size;
        } else {
            old_value_.size = 0;
            old_value_.data = nullptr;
            old_value_.allocated_ = false;
        }
        int new_size = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        if (new_size > 0) {
            new_value_.Deserialize(src + offset - sizeof(int));
        } else {
            new_value_.size = 0;
            new_value_.data = nullptr;
            new_value_.allocated_ = false;
        }
    }
};

class InsertLogRecord: public RecordLogRecord {
public:
    InsertLogRecord() = default;
    InsertLogRecord(txn_id_t txn_id, RmRecord& insert_value, Rid& rid, std::string table_name)
        : RecordLogRecord(LogType::INSERT, txn_id, table_name, rid, nullptr, &insert_value) {}
};

class DeleteLogRecord: public RecordLogRecord {
public:
    DeleteLogRecord() = default;
    DeleteLogRecord(txn_id_t txn_id, RmRecord& delete_value, Rid& rid, std::string table_name)
        : RecordLogRecord(LogType::DELETE, txn_id, table_name, rid, &delete_value, nullptr) {}
};

class UpdateLogRecord: public RecordLogRecord {
public:
    UpdateLogRecord() = default;
    UpdateLogRecord(txn_id_t txn_id, RmRecord& old_value, RmRecord& new_value, Rid& rid, std::string table_name)
        : RecordLogRecord(LogType::UPDATE, txn_id, table_name, rid, &old_value, &new_value) {}
};

class LogBuffer {
public:
    LogBuffer() {
        offset_ = 0;
        memset(buffer_, 0, sizeof(buffer_));
    }

    bool is_full(int append_size) { return offset_ + append_size > LOG_BUFFER_SIZE; }

    char buffer_[LOG_BUFFER_SIZE + 1];
    int offset_;
};

class LogManager {
public:
    explicit LogManager(DiskManager* disk_manager) { disk_manager_ = disk_manager; }

    lsn_t add_log_to_buffer(LogRecord* log_record);
    void flush_log_to_disk();

    LogBuffer* get_log_buffer() { return &log_buffer_; }

private:
    std::atomic<lsn_t> global_lsn_{0};
    std::mutex latch_;
    LogBuffer log_buffer_;
    lsn_t persist_lsn_ = INVALID_LSN;
    DiskManager* disk_manager_;
};
