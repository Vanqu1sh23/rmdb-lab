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

#include <cmath>
#include <cstdint>
#include <limits>

#include "execution_defs.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    static bool is_numeric_type(ColType type) {
        return type == TYPE_INT || type == TYPE_BIGINT || type == TYPE_FLOAT;
    }

    static bool is_ordered_i64_type(ColType type) {
        return type == TYPE_INT || type == TYPE_BIGINT || type == TYPE_DATETIME;
    }

    static int64_t raw_to_i64(const char *data, ColType type) {
        if (type == TYPE_INT) return *reinterpret_cast<const int *>(data);
        if (type == TYPE_BIGINT || type == TYPE_DATETIME) return *reinterpret_cast<const int64_t *>(data);
        return static_cast<int64_t>(*reinterpret_cast<const float *>(data));
    }

    static long double raw_to_numeric(const char *data, ColType type) {
        if (type == TYPE_INT) return *reinterpret_cast<const int *>(data);
        if (type == TYPE_BIGINT || type == TYPE_DATETIME) return *reinterpret_cast<const int64_t *>(data);
        return *reinterpret_cast<const float *>(data);
    }

    static int compare_raw(const char *lhs, const char *rhs, ColType lhs_type, ColType rhs_type, int len) {
        if (lhs_type == TYPE_DATETIME && rhs_type == TYPE_DATETIME) {
            int64_t l = raw_to_i64(lhs, lhs_type);
            int64_t r = raw_to_i64(rhs, rhs_type);
            return (l > r) - (l < r);
        }
        if (is_numeric_type(lhs_type) && is_numeric_type(rhs_type)) {
            if (lhs_type != TYPE_FLOAT && rhs_type != TYPE_FLOAT) {
                int64_t l = raw_to_i64(lhs, lhs_type);
                int64_t r = raw_to_i64(rhs, rhs_type);
                return (l > r) - (l < r);
            }
            long double l = raw_to_numeric(lhs, lhs_type);
            long double r = raw_to_numeric(rhs, rhs_type);
            constexpr long double eps = 1e-6L;
            if (std::fabs(l - r) <= eps) return 0;
            return l > r ? 1 : -1;
        }
        return strncmp(lhs, rhs, len);
    }

    static bool compare_result(int cmp, CompOp op) {
        switch (op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
        }
        return false;
    }

    bool eval_cond(const Condition &cond, const std::vector<ColMeta> &rec_cols, const RmRecord *rec) {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        const char *lhs = rec->data + lhs_col->offset;
        const char *rhs = nullptr;
        ColType rhs_type = lhs_col->type;
        int cmp_len = lhs_col->len;
        if (cond.is_rhs_val) {
            rhs = cond.rhs_val.raw->data;
            rhs_type = cond.rhs_val.type;
        } else {
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs = rec->data + rhs_col->offset;
            rhs_type = rhs_col->type;
            cmp_len = std::min(lhs_col->len, rhs_col->len);
        }
        return compare_result(compare_raw(lhs, rhs, lhs_col->type, rhs_type, cmp_len), cond.op);
    }

    bool eval_conds(const std::vector<Condition> &conds, const std::vector<ColMeta> &rec_cols, const RmRecord *rec) {
        for (const auto &cond : conds) {
            if (!eval_cond(cond, rec_cols, rec)) return false;
        }
        return true;
    }

    static void cast_value_to_col(Value &val, const ColMeta &col) {
        if (val.type == col.type) return;
        if (col.type == TYPE_DATETIME && val.type == TYPE_STRING) {
            val.set_datetime(parse_datetime_literal(val.str_val));
            return;
        }
        if (!is_numeric_type(val.type) || !is_numeric_type(col.type)) {
            throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
        }
        if (col.type == TYPE_FLOAT) {
            val.set_float(static_cast<float>(val.type == TYPE_BIGINT ? val.bigint_val : val.int_val));
        } else {
            int64_t value = val.type == TYPE_FLOAT ? static_cast<int64_t>(val.float_val)
                                                   : (val.type == TYPE_BIGINT ? val.bigint_val : val.int_val);
            if (col.type == TYPE_INT) {
                if (value < std::numeric_limits<int>::min() || value > std::numeric_limits<int>::max()) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                val.set_int(static_cast<int>(value));
            } else {
                val.set_bigint(value);
            }
        }
    }
};
