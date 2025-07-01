# 更新日志

## [Unreleased]

### 新增

- 在语义分析阶段添加了对 `ORDER BY` 子句的检查。当存在 `GROUP BY` 子句时，`ORDER BY` 的列必须同时存在于 `SELECT` 或 `GROUP BY` 中。