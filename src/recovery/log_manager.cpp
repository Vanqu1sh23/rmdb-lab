/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_manager.h"

#include <cstring>
#include <unistd.h>

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    if (log_record == nullptr) return INVALID_LSN;
    std::unique_lock<std::mutex> lock(latch_);
    if (log_buffer_.is_full(static_cast<int>(log_record->log_tot_len_))) {
        lock.unlock();
        flush_log_to_disk();
        lock.lock();
    }
    log_record->lsn_ = global_lsn_++;
    std::vector<char> data(log_record->log_tot_len_);
    log_record->serialize(data.data());
    memcpy(log_buffer_.buffer_ + log_buffer_.offset_, data.data(), log_record->log_tot_len_);
    log_buffer_.offset_ += log_record->log_tot_len_;
    return log_record->lsn_;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中
 */
void LogManager::flush_log_to_disk() {
    std::unique_lock<std::mutex> lock(latch_);
    if (log_buffer_.offset_ == 0) return;
    disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
    if (disk_manager_->GetLogFd() != -1) {
        fsync(disk_manager_->GetLogFd());
    }
    persist_lsn_ = global_lsn_ - 1;
    memset(log_buffer_.buffer_, 0, sizeof(log_buffer_.buffer_));
    log_buffer_.offset_ = 0;
}
