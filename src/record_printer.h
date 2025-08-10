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

#include <cassert>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/context.h"
#include "common/ConstexprString.h"

#define RECORD_COUNT_LENGTH 40

class RecordPrinter {
    size_t num_cols;

   public:
    RecordPrinter(size_t num_cols_) : num_cols(num_cols_) { assert(num_cols_ > 0); }

    void print_separator(Context *context) const {
        int &offset = *(context->offset_);
        char *buffer = context->data_send_ + offset;

        for (size_t i = 0; i < num_cols; i++) {
            if (context->ellipsis_ == false && offset + RECORD_COUNT_LENGTH + COL_WIDTH + 3 < BUFFER_LENGTH) {
                buffer[0] = '+';
                memset(buffer + 1, '-', COL_WIDTH + 2);
                offset += COL_WIDTH + 3;
                buffer += COL_WIDTH + 3;
            } else {
                context->ellipsis_ = true;
            }
        }
        if (context->ellipsis_ == false && offset + RECORD_COUNT_LENGTH + 2 < BUFFER_LENGTH) {
            buffer[0] = '+';
            buffer[1] = '\n';
            offset += 2;
        } else {
            context->ellipsis_ = true;
        }
    }

    void print_record(const std::vector<std::string_view> &rec_str, Context *context) const {
        int &offset = *(context->offset_);
        char *buffer = context->data_send_ + offset;

        for (auto col : rec_str) {
            if (context->ellipsis_) return;
            if (offset + RECORD_COUNT_LENGTH + COL_WIDTH + 3 >= BUFFER_LENGTH) {
                context->ellipsis_ = true;
                break;
            }
            strcpy(buffer, "| ");
            if (col.size() > COL_WIDTH) {
                memcpy(buffer + 2, col.data(), COL_WIDTH - 3);
                strcpy(buffer, "... ");
            } else {
                memset(buffer + 2, ' ', COL_WIDTH - col.size());
                memcpy(buffer + 2 + COL_WIDTH - col.size(), col.data(), col.size());
                buffer[COL_WIDTH + 2] = ' ';
            }

            buffer += COL_WIDTH + 3;
            offset += COL_WIDTH + 3;
        }

        if (context->ellipsis_ == false && offset + RECORD_COUNT_LENGTH + 2 < BUFFER_LENGTH) {
            buffer[0] = '|';
            buffer[1] = '\n';
            offset += 2;
        } else {
            context->ellipsis_ = true;
        }
    }

    static void print_record_count(size_t num_rec, Context *context) {
        int &offset = *(context->offset_);
        char *buffer = context->data_send_ + offset;

        if (context->ellipsis_) {
            sprintf(buffer, "... ...\n");

            buffer += 8;
            offset += 8;
        }

        size_t size = sprintf(buffer, "Total record(s): %ld\n", num_rec);
        offset += size;
    }
};
