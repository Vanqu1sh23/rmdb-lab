/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>

#include "execution_manager.h"

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

const char *help_info = "Supported SQL syntax:\n"
                   "  command ;\n"
                   "command:\n"
                   "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
                   "  DROP TABLE table_name\n"
                   "  CREATE INDEX table_name (column_name)\n"
                   "  DROP INDEX table_name (column_name)\n"
                   "  INSERT INTO table_name VALUES (value [, value ...])\n"
                   "  DELETE FROM table_name [WHERE where_clause]\n"
                   "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
                   "  SELECT selector FROM table_name [WHERE where_clause]\n"
                   "type:\n"
                   "  {INT | FLOAT | CHAR(n)}\n"
                   "where_clause:\n"
                   "  condition [AND condition ...]\n"
                   "condition:\n"
                   "  column op {column | value}\n"
                   "column:\n"
                   "  [table_name.]column_name\n"
                   "op:\n"
                   "  {= | <> | < | > | <= | >=}\n"
                   "selector:\n"
                   "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context){
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch(x->tag) {
            case T_CreateTable:
            {
                sm_manager_->create_table(x->tab_name_, x->cols_, context);
                break;
            }
            case T_DropTable:
            {
                sm_manager_->drop_table(x->tab_name_, context);
                break;
            }
            case T_CreateIndex:
            {
                sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            case T_DropIndex:
            {
                sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;  
        }
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch(x->tag) {
            case T_Help:
            {
                memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
                *(context->offset_) = strlen(help_info);
                break;
            }
            case T_ShowTable:
            {
                sm_manager_->show_tables(context);
                break;
            }
            case T_DescTable:
            {
                sm_manager_->desc_table(x->tab_name_, context);
                break;
            }
            case T_ShowIndex:
            {
                sm_manager_->show_index(x->tab_name_, context);
                break;
            }
            case T_Transaction_begin:
            {
                // 显示开启一个事务
                context->txn_->set_txn_mode(true);
                break;
            }  
            case T_Transaction_commit:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->commit(context->txn_, context->log_mgr_);
                break;
            }    
            case T_Transaction_rollback:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context->txn_, context->log_mgr_);
                break;
            }    
            case T_Transaction_abort:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context->txn_, context->log_mgr_);
                break;
            }     
            default:
                throw InternalError("Unexpected field type");
                break;                        
        }

    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
namespace {
std::string value_to_output_string(const char *rec_buf, ColType type, int len) {
    if (type == TYPE_INT) return std::to_string(*(int *)rec_buf);
    if (type == TYPE_BIGINT) return std::to_string(*(int64_t *)rec_buf);
    if (type == TYPE_FLOAT) return std::to_string(*(float *)rec_buf);
    if (type == TYPE_STRING) {
        std::string col_str((char *)rec_buf, len);
        col_str.resize(strlen(col_str.c_str()));
        return col_str;
    }
    if (type == TYPE_DATETIME) return datetime_to_string(*(int64_t *)rec_buf);
    return "";
}

long double numeric_value(const char *rec_buf, ColType type) {
    if (type == TYPE_INT) return *(int *)rec_buf;
    if (type == TYPE_BIGINT) return *(int64_t *)rec_buf;
    if (type == TYPE_FLOAT) return *(float *)rec_buf;
    return 0;
}

void print_output_row(const std::vector<std::string> &columns, Context *context, std::fstream &outfile, const RecordPrinter &printer) {
    printer.print_record(columns, context);
    outfile << "|";
    for (auto &column : columns) outfile << " " << column << " |";
    outfile << "\n";
}
}

void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, 
                            Context *context) {
    bool has_agg = false;
    for (auto &sel_col : sel_cols) {
        if (sel_col.agg_type != AggType::NONE) {
            has_agg = true;
            break;
        }
    }

    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(has_agg ? sel_col.alias : sel_col.col_name);
    }

    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "|";
    for (auto &caption : captions) outfile << " " << caption << " |";
    outfile << "\n";

    if (has_agg) {
        struct AggState {
            bool seen = false;
            long double numeric = 0;
            std::string text;
            ColType type = TYPE_INT;
        };
        std::vector<AggState> states(sel_cols.size());
        size_t row_count = 0;
        auto &input_cols = executorTreeRoot->cols();
        std::vector<std::vector<ColMeta>::const_iterator> target_cols(sel_cols.size(), input_cols.end());
        for (size_t i = 0; i < sel_cols.size(); ++i) {
            if (!(sel_cols[i].agg_type == AggType::COUNT && sel_cols[i].is_star)) {
                target_cols[i] = executorTreeRoot->get_col(input_cols, sel_cols[i]);
                states[i].type = target_cols[i]->type;
            }
        }

        for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
            auto tuple = executorTreeRoot->Next();
            row_count++;
            for (size_t i = 0; i < sel_cols.size(); ++i) {
                auto &sel_col = sel_cols[i];
                auto &state = states[i];
                if (sel_col.agg_type == AggType::COUNT) continue;
                const auto &col = *target_cols[i];
                const char *buf = tuple->data + col.offset;
                if (sel_col.agg_type == AggType::SUM) {
                    state.numeric += numeric_value(buf, col.type);
                    state.seen = true;
                } else if (sel_col.agg_type == AggType::MAX || sel_col.agg_type == AggType::MIN) {
                    std::string text = value_to_output_string(buf, col.type, col.len);
                    long double num = numeric_value(buf, col.type);
                    bool better = !state.seen;
                    if (col.type == TYPE_STRING) {
                        if (sel_col.agg_type == AggType::MAX) better = better || text > state.text;
                        else better = better || text < state.text;
                    } else {
                        if (sel_col.agg_type == AggType::MAX) better = better || num > state.numeric;
                        else better = better || num < state.numeric;
                    }
                    if (better) {
                        state.numeric = num;
                        state.text = text;
                        state.seen = true;
                    }
                }
            }
        }

        std::vector<std::string> columns;
        for (size_t i = 0; i < sel_cols.size(); ++i) {
            auto &sel_col = sel_cols[i];
            auto &state = states[i];
            if (sel_col.agg_type == AggType::COUNT) {
                columns.push_back(std::to_string(row_count));
            } else if (sel_col.agg_type == AggType::SUM) {
                if (state.type == TYPE_FLOAT) columns.push_back(std::to_string(static_cast<float>(state.numeric)));
                else columns.push_back(std::to_string(static_cast<int64_t>(state.numeric)));
            } else if (sel_col.agg_type == AggType::MAX || sel_col.agg_type == AggType::MIN) {
                if (state.type == TYPE_STRING || state.type == TYPE_DATETIME) columns.push_back(state.text);
                else if (state.type == TYPE_FLOAT) columns.push_back(std::to_string(static_cast<float>(state.numeric)));
                else columns.push_back(std::to_string(static_cast<int64_t>(state.numeric)));
            }
        }
        print_output_row(columns, context, outfile, rec_printer);
        outfile.close();
        rec_printer.print_separator(context);
        RecordPrinter::print_record_count(1, context);
        return;
    }

    size_t num_rec = 0;
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols()) {
            columns.push_back(value_to_output_string(Tuple->data + col.offset, col.type, col.len));
        }
        print_output_row(columns, context, outfile, rec_printer);
        num_rec++;
    }
    outfile.close();
    rec_printer.print_separator(context);
    RecordPrinter::print_record_count(num_rec, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec){
    exec->Next();
}