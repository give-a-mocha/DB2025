# RMDB 查询执行引擎说明

## 1. 概述

RMDB的查询执行引擎负责将查询优化器生成的执行计划转换为具体的执行器，并协调各个执行器的运行以产生查询结果。执行引擎采用迭代器模型（Iterator Model），也称为火山模型（Volcano Model），通过统一的接口实现不同算子的组合和数据流传递。

## 2. 执行引擎架构

### 2.1 核心组件

```
Portal (计划转换器)
    ↓
QlManager (执行管理器)
    ↓
AbstractExecutor (执行器抽象基类)
    ↓
具体执行器 (SeqScan, IndexScan, Join, Projection等)
```

### 2.2 执行流程

1. **计划转换**：Portal将Plan节点递归转换为AbstractExecutor树
2. **执行协调**：QlManager根据语句类型调用相应的执行方法
3. **迭代执行**：通过beginTuple()、nextTuple()、Next()接口迭代产生结果
4. **结果输出**：将执行结果格式化输出到客户端和文件

## 3. 执行器类型和功能

### 3.1 数据访问执行器

#### SeqScanExecutor（顺序扫描执行器）
- **功能**：顺序扫描表中的所有记录，应用过滤条件
- **适用场景**：全表扫描、无可用索引的查询
- **实现特点**：
  - 使用RmScan进行表遍历
  - 在beginTuple()和nextTuple()中应用条件过滤
  - 支持多种数据类型的条件评估

#### IndexScanExecutor（索引扫描执行器）
- **功能**：基于索引进行高效的数据访问
- **适用场景**：有索引可用且查询条件适合索引的情况
- **实现特点**：
  - 利用索引减少扫描的数据量
  - 支持等值查询和范围查询
  - 应用最左匹配原则

### 3.2 连接执行器

#### NestedLoopJoinExecutor（嵌套循环连接执行器）
- **功能**：实现两个关系的嵌套循环连接
- **算法描述**：
  ```
  FOR each record r in left_relation:
      FOR each record s in right_relation:
          IF join_condition(r, s):
              OUTPUT concatenate(r, s)
  ```
- **实现特点**：
  - 外层循环遍历左子执行器
  - 内层循环遍历右子执行器
  - 动态调整列偏移量以正确合并记录
  - 支持多个连接条件的组合评估

#### SortMergeJoinExecutor（排序合并连接执行器）
- **功能**：实现基于排序的合并连接算法
- **适用场景**：大表连接，特别是等值连接
- **算法优势**：时间复杂度O(n log n + m log m)，优于嵌套循环的O(n×m)

### 3.3 数据处理执行器

#### ProjectionExecutor（投影执行器）
- **功能**：从输入记录中选择指定的列
- **实现细节**：
  - 重新计算列偏移量以紧密排列数据
  - 支持列重排序和选择性投影
  - 优化内存使用，只保留需要的列

#### SortExecutor（排序执行器）
- **功能**：对查询结果进行排序
- **实现特点**：
  - 支持多列排序
  - 支持升序和降序混合排序
  - 采用内存排序或外部排序（基于数据量）

#### AggregateExecutor（聚合执行器）
- **功能**：执行聚合函数计算
- **支持的聚合函数**：
  - COUNT()：计数
  - SUM()：求和
  - AVG()：平均值
  - MAX()：最大值
  - MIN()：最小值

#### GroupExecutor（分组执行器）
- **功能**：实现GROUP BY分组操作
- **实现策略**：
  - 基于哈希的分组（适合内存能容纳所有分组的情况）
  - 基于排序的分组（适合大数据量分组）

### 3.4 数据修改执行器

#### InsertExecutor（插入执行器）
- **功能**：向表中插入新记录
- **实现要点**：
  - 验证数据类型和约束
  - 更新相关索引
  - 记录事务日志

#### UpdateExecutor（更新执行器）
- **功能**：更新现有记录
- **实现流程**：
  1. 通过扫描执行器定位目标记录
  2. 应用SET子句修改记录
  3. 更新相关索引
  4. 记录事务日志

#### DeleteExecutor（删除执行器）
- **功能**：删除满足条件的记录
- **实现流程**：
  1. 通过扫描执行器定位目标记录
  2. 删除记录并更新索引
  3. 记录事务日志

## 4. 迭代器模型接口

### 4.1 核心接口

```cpp
class AbstractExecutor {
public:
    virtual void beginTuple() = 0;        // 初始化迭代器，定位到第一条记录
    virtual void nextTuple() = 0;         // 移动到下一条记录
    virtual bool is_end() const = 0;      // 检查是否已到达末尾
    virtual std::unique_ptr<RmRecord> Next() = 0;  // 获取当前记录
    virtual size_t tupleLen() const = 0;  // 获取记录长度
    virtual const std::vector<ColMeta>& cols() const = 0;  // 获取列元数据
};
```

### 4.2 执行器生命周期

```
创建执行器 → beginTuple() → [nextTuple() → Next()]* → 结束
```

### 4.3 条件评估

```cpp
bool eval_conds(const std::vector<ColMeta>& rec_cols, 
                const std::vector<Condition>& conds, 
                const RmRecord* rec);
```

## 5. Portal系统：计划到执行器的转换

### 5.1 Portal类职责
- **计划转换**：将Plan树递归转换为Executor树
- **执行协调**：根据语句类型选择合适的执行策略
- **资源管理**：管理执行器的生命周期

### 5.2 转换策略

#### convert_plan_executor函数
```cpp
std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context* context) {
    // 根据Plan类型创建对应的Executor
    if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        return std::make_unique<ProjectionExecutor>(
            convert_plan_executor(x->subplan_, context), 
            x->sel_cols_);
    }
    // ... 其他Plan类型的转换
}
```

### 5.3 Portal标签类型

```cpp
enum portalTag {
    PORTAL_ONE_SELECT,          // 单个SELECT查询
    PORTAL_DML_WITHOUT_SELECT,  // 不返回结果的DML操作
    PORTAL_MULTI_QUERY,         // DDL等复合查询
    PORTAL_CMD_UTILITY         // 工具命令（HELP、SHOW等）
};
```

## 6. QlManager：执行管理器

### 6.1 主要功能

#### select_from函数
- **功能**：执行SELECT查询并输出结果
- **实现流程**：
  1. 打印表头
  2. 迭代执行器获取记录
  3. 格式化输出到客户端和文件
  4. 统计记录数量

#### run_dml函数
- **功能**：执行INSERT/UPDATE/DELETE操作
- **特点**：只调用executor的Next()方法，不输出结果

#### run_mutli_query函数
- **功能**：执行DDL语句
- **操作类型**：创建/删除表、创建/删除索引

#### run_cmd_utility函数
- **功能**：执行工具命令和事务控制
- **支持命令**：HELP、SHOW、DESC、BEGIN、COMMIT、ROLLBACK

## 7. 性能优化策略

### 7.1 迭代器模型优势
- **流水线处理**：上层算子可以在下层算子产生数据时立即开始处理
- **内存效率**：不需要缓存所有中间结果
- **可组合性**：不同算子可以灵活组合

### 7.2 具体优化技术

#### 条件下推
- 在扫描阶段就应用过滤条件
- 减少上层算子需要处理的数据量

#### 投影下推
- 尽早减少记录宽度
- 降低内存使用和I/O开销

#### 索引利用
- 根据查询条件选择合适的索引
- 减少需要扫描的数据量

#### 连接顺序优化
- 将选择性高的操作放在前面
- 减少连接中间结果的大小

## 8. 错误处理和调试

### 8.1 常见错误类型
- **ColumnNotFoundError**：列不存在
- **IncompatibleTypeError**：类型不兼容
- **InternalError**：内部逻辑错误

### 8.2 调试方法
- **getType()方法**：每个执行器都有类型标识
- **错误消息**：包含执行器类型信息便于定位
- **断点调试**：在关键的迭代器方法设置断点

## 9. 扩展和改进方向

### 9.1 性能改进
- **向量化执行**：批量处理多条记录
- **代码生成**：编译时生成特化的执行代码
- **并行执行**：利用多核进行并行查询处理

### 9.2 功能扩展
- **更多连接算法**：哈希连接、索引嵌套循环连接
- **更复杂的聚合**：窗口函数、CUBE/ROLLUP
- **子查询支持**：相关子查询和EXISTS操作

### 9.3 可靠性增强
- **内存管理**：更精细的内存控制和溢出处理
- **异常恢复**：更完善的错误恢复机制
- **资源限制**：查询超时和资源使用限制

## 10. 使用示例

### 10.1 简单查询执行流程
```sql
SELECT name, age FROM students WHERE age > 20;
```

**执行器树结构**：
```
ProjectionExecutor(name, age)
  └── SeqScanExecutor(students, [age > 20])
```

### 10.2 连接查询执行流程
```sql
SELECT s.name, c.title 
FROM students s, courses c 
WHERE s.id = c.student_id AND s.age > 20;
```

**执行器树结构**：
```
ProjectionExecutor(s.name, c.title)
  └── NestedLoopJoinExecutor(s.id = c.student_id)
        ├── SeqScanExecutor(students, [age > 20])
        └── SeqScanExecutor(courses, [])
```

### 10.3 聚合查询执行流程
```sql
SELECT dept, COUNT(*), AVG(salary) 
FROM employees 
GROUP BY dept 
HAVING COUNT(*) > 5;
```

**执行器树结构**：
```
ProjectionExecutor(dept, COUNT(*), AVG(salary))
  └── AggregateExecutor(COUNT(*), AVG(salary))
        └── GroupExecutor(GROUP BY dept, HAVING COUNT(*) > 5)
              └── SeqScanExecutor(employees, [])
```

---

*此文档描述了RMDB查询执行引擎的完整架构和实现细节。执行引擎是数据库系统的核心组件，负责高效地执行各种数据库操作。*