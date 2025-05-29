#include "execution_common.h"
#include "system/sm.h"

auto ReconstructTuple(const TabMeta *schema, const RmRecord &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<RmRecord> {
    // 如果元组已被删除，返回空
    if (base_meta.is_deleted_) {
        return std::nullopt;
    }
    
    // 创建结果记录的副本
    RmRecord result(base_tuple.size);
    memcpy(result.data, base_tuple.data, base_tuple.size);
    
    // 应用撤销日志，从最新到最旧
    for (auto it = undo_logs.rbegin(); it != undo_logs.rend(); ++it) {
        const auto &undo_log = *it;
        
        if (undo_log.is_deleted_) {
            // 如果撤销日志标记为删除，返回空
            return std::nullopt;
        }
        
        // 应用撤销日志中的字段修改
        if (undo_log.tuple_test_) {
            memcpy(result.data, undo_log.tuple_test_->data, result.size);
        } else if (!undo_log.tuple_.empty()) {
            // 使用Value数组重建记录
            for (size_t i = 0; i < undo_log.tuple_.size() && i < schema->cols.size(); ++i) {
                if (i < undo_log.modified_fields_.size() && undo_log.modified_fields_[i]) {
                    const auto &col = schema->cols[i];
                    const auto &val = undo_log.tuple_[i];
                    if (val.raw && val.raw->data) {
                        memcpy(result.data + col.offset, val.raw->data, col.len);
                    }
                }
            }
        }
    }
    
    return result;
}

auto IsWriteWriteConflict(timestamp_t tuple_ts, Transaction *txn) -> bool {
    // 检查写-写冲突
    // 如果元组的时间戳大于事务的开始时间戳，则存在写-写冲突
    return tuple_ts > txn->get_start_ts();
}

auto message_out(Context *context_, const std::string &output) -> void {
    if (context_ && context_->data_send_ && context_->offset_ &&
        *context_->offset_ + output.length() < BUFFER_LENGTH) {
        memcpy(context_->data_send_ + *context_->offset_, output.c_str(), output.length());
        *context_->offset_ += output.length();
    }
}

auto message_out(Context *context_, const char *output, size_t output_size) -> void {
    if (context_ && context_->data_send_ && context_->offset_ &&
        *context_->offset_ + output_size < BUFFER_LENGTH) {
        memcpy(context_->data_send_ + *context_->offset_, output, output_size);
        *context_->offset_ += output_size;
    }
}
