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

#include <iostream>
#include <map>

// 此处重载了<<操作符，在ColMeta中进行了调用
template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
    os << static_cast<int>(enum_val);
    return os;
}

template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

struct Rid {
    int page_no;
    int slot_no;

    friend bool operator==(const Rid &x, const Rid &y) {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
};

enum class ColType {
    TYPE_INT, TYPE_FLOAT, TYPE_STRING
};

inline std::string coltype2str(ColType type) {
    switch(type) {
        case ColType::TYPE_INT:
            return "INT";
        case ColType::TYPE_FLOAT:
            return "FLOAT";
        case ColType::TYPE_STRING:
            return "STRING";
        default:
            throw std::runtime_error("Unknown column type");
    }
}

enum AggregateType {
    NONE, AGG_COUNT, AGG_MAX, AGG_MIN, AGG_SUM, AGG_AVG
};

// 辅助函数：将字符串转换为聚合类型
inline AggregateType str2aggregate(const std::string& str) {
    if (str == "COUNT") return AGG_COUNT;
    if (str == "MAX") return AGG_MAX;
    if (str == "MIN") return AGG_MIN;
    if (str == "SUM") return AGG_SUM;
    if (str == "AVG") return AGG_AVG;
    return NONE;
}

// 辅助函数：将聚合类型转换为小写字符串
inline std::string aggregate2str(AggregateType agg) {
    switch (agg) {
        case AGG_COUNT: return "count";
        case AGG_MAX: return "max";
        case AGG_MIN: return "min";
        case AGG_SUM: return "sum";
        case AGG_AVG: return "avg";
        default: return "";
    }
}

class RecScan {
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual Rid rid() const = 0;
};
