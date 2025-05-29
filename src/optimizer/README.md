# RMDB 查询执行计划构建说明

## 1. 概述

RMDB的查询执行计划是一个树形结构，由多个计划节点（Plan Node）组成。每个节点代表一个特定的数据库操作（如扫描、连接、聚合等）。计划树的构建过程发生在查询优化器中，通过逻辑优化和物理优化两个阶段来生成高效的执行计划。

## 2. 计划节点类型（PlanTag）

### 2.1 数据定义语言（DDL）
- `T_CreateTable`: 创建表
- `T_DropTable`: 删除表  
- `T_CreateIndex`: 创建索引
- `T_DropIndex`: 删除索引

### 2.2 数据操作语言（DML）
- `T_Insert`: 插入数据
- `T_Update`: 更新数据
- `T_Delete`: 删除数据
- `T_select`: 查询数据

### 2.3 执行算子
- `T_SeqScan`: 顺序扫描
- `T_IndexScan`: 索引扫描
- `T_NestLoop`: 嵌套循环连接
- `T_SortMerge`: 排序合并连接
- `T_Sort`: 排序
- `T_Projection`: 投影
- `T_Aggregate`: 聚合
- `T_Group`: 分组

### 2.4 其他操作
- `T_Help`: 帮助信息
- `T_ShowTable`: 显示表
- `T_ShowIndex`: 显示索引
- `T_DescTable`: 描述表结构
- `T_SetKnob`: 设置参数
- `T_Transaction_begin/commit/abort/rollback`: 事务控制

## 3. 计划节点类层次结构

### 3.1 基础计划类
```cpp
class Plan {
public:
    PlanTag tag;              // 计划节点类型标识
    virtual ~Plan() = default;
};
```

### 3.2 扫描计划（ScanPlan）
负责数据扫描操作，支持顺序扫描和索引扫描：
- `tab_name_`: 表名
- `cols_`: 表的列元数据
- `conds_`: 扫描条件
- `len_`: 记录长度
- `fed_conds_`: 提供给执行器的条件
- `index_col_names_`: 索引列名（用于索引扫描）

### 3.3 连接计划（JoinPlan）
负责表之间的连接操作：
- `left_`: 左子计划
- `right_`: 右子计划
- `conds_`: 连接条件
- `type`: 连接类型（目前支持内连接）

### 3.4 投影计划（ProjectionPlan）
负责列的选择和投影：
- `subplan_`: 子计划
- `sel_cols_`: 需要投影的列

### 3.5 排序计划（SortPlan）
负责结果排序：
- `subplan_`: 子计划
- `sel_col_`: 排序列
- `is_desc_`: 是否降序排序

### 3.6 DML计划（DMLPlan）
负责数据操作语句：
- `subplan_`: 子计划
- `tab_name_`: 表名
- `values_`: 插入值
- `conds_`: 条件
- `set_clauses_`: 更新子句

### 3.7 DDL计划（DDLPlan）
负责数据定义语句：
- `tab_name_`: 表名
- `tab_col_names_`: 列名列表
- `cols_`: 列定义

### 3.8 聚合计划（AggregatePlan）
负责聚合操作：
- `subplan_`: 子计划
- `sel_cols_`: 选择列

### 3.9 分组计划（GroupPlan）
负责分组操作：
- `subplan_`: 子计划
- `sel_cols_`: 选择列
- `group_cols_`: 分组列
- `having_conds_`: HAVING条件

## 4. 查询计划构建流程

### 4.1 总体流程
1. **语法分析**: 将SQL语句解析为AST（抽象语法树）
2. **语义分析**: 检查语法正确性，生成Query对象
3. **逻辑优化**: 应用逻辑优化规则
4. **物理优化**: 选择具体的执行算法和访问路径
5. **计划生成**: 构建最终的执行计划树

### 4.2 入口函数
```cpp
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context)
```
根据查询类型分发到不同的计划生成函数。

### 4.3 SELECT查询计划生成
```cpp
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context)
```
1. 逻辑优化
2. 物理优化  
3. 添加投影节点

## 5. 优化策略

### 5.1 逻辑优化（logical_optimization）
当前实现的优化规则：
- **条件下推**: 将单表条件尽早执行，在`make_one_rel`中通过`pop_conds`实现
- **谓词简化**: 去除恒真或恒假条件（TODO）
- **常量折叠**: 预计算常量表达式（TODO）
- **连接重排序**: 将小表放在外层（需要统计信息支持）

### 5.2 物理优化（physical_optimization）
1. **基础连接计划生成**: 调用`make_one_rel`
2. **索引选择优化**: 通过`get_index_cols`实现最左匹配原则
3. **连接算法选择**: 支持嵌套循环连接和排序合并连接
4. **分组处理**: `generate_group_plan`
5. **聚合处理**: `generate_aggregate_plan`  
6. **排序处理**: `generate_sort_plan`

## 6. 核心算法详解

### 6.1 索引选择算法（get_index_cols）
实现最左匹配原则的索引选择：
```cpp
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition> curr_conds, 
                             std::vector<std::string> &index_col_names)
```

**算法流程**:
1. 从查询条件中提取涉及目标表的列
2. 支持等值条件（`OP_EQ`）和范围条件（`OP_GT`, `OP_GE`, `OP_LT`, `OP_LE`）
3. 遍历所有索引，找到最匹配的索引（连续匹配列数最多）
4. 应用最左匹配原则：如果某列不匹配则停止

### 6.2 条件下推算法（push_conds）
将连接条件推送到查询计划树的合适位置：
```cpp
int push_conds(Condition *cond, std::shared_ptr<Plan> plan)
```

**返回值含义**:
- `0`: 条件不涉及当前计划的任何表
- `1`: 条件的左操作数列匹配当前计划
- `2`: 条件的右操作数列匹配当前计划  
- `3`: 条件已被完全处理并关联到计划节点

**算法逻辑**:
- 对于`ScanPlan`: 检查条件列是否属于扫描的表
- 对于`JoinPlan`: 递归下推到左右子节点，如果两个子节点都匹配则在当前节点处理连接条件

### 6.3 多表连接计划生成（make_one_rel）
核心的连接计划构建算法：

**第一阶段 - 单表扫描计划生成**:
1. 为每个表提取适用的单表条件（`pop_conds`）
2. 选择访问路径：索引扫描 vs 顺序扫描（`get_index_cols`）
3. 生成对应的`ScanPlan`

**第二阶段 - 连接计划生成**:
1. 处理连接条件，构建第一层连接
2. 根据配置选择连接算法（嵌套循环 vs 排序合并）
3. 递归处理剩余连接条件
4. 使用笛卡尔积连接没有条件的表

### 6.4 条件提取算法（pop_conds）
从全局条件列表中提取适用于特定表的条件：
```cpp
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names)
```

**提取规则**:
- 单表条件：`table.col op value`
- 自连接条件：`table.col1 op table.col2`

## 7. 配置选项

### 7.1 连接算法选择
- `enable_nestedloop_join`: 启用嵌套循环连接
- `enable_sortmerge_join`: 启用排序合并连接
- 当两个都启用时，默认使用嵌套循环连接

### 7.2 扩展点
- **成本模型**: 可添加基于成本的优化器
- **统计信息**: 支持表和索引的统计信息
- **更多连接算法**: 如哈希连接
- **并行执行**: 支持并行查询执行

## 8. 使用示例

### 8.1 简单查询
```sql
SELECT name, age FROM students WHERE age > 20;
```
**生成的计划树**:
```
ProjectionPlan(name, age)
  └── ScanPlan(students, [age > 20])
```

### 8.2 连接查询  
```sql
SELECT s.name, c.title 
FROM students s, courses c 
WHERE s.id = c.student_id AND s.age > 20;
```
**生成的计划树**:
```
ProjectionPlan(s.name, c.title)
  └── JoinPlan(s.id = c.student_id)
        ├── ScanPlan(students, [age > 20])  
        └── ScanPlan(courses, [])
```

### 8.3 排序查询
```sql
SELECT * FROM students ORDER BY age DESC, name ASC;
```
**生成的计划树**:
```
ProjectionPlan(*)
  └── SortPlan([age DESC, name ASC])
        └── ScanPlan(students, [])
```

## 9. 性能考虑

### 9.1 索引使用
- 优先选择覆盖索引
- 应用最左匹配原则
- 支持复合索引的部分匹配

### 9.2 连接顺序
- 目前使用启发式规则
- 未来可引入基于成本的选择

### 9.3 内存管理
- 使用智能指针管理计划节点
- 支持移动语义减少拷贝开销

## 10. 调试和监控

### 10.1 计划打印
可通过遍历计划树打印执行计划，便于调试和性能分析。

### 10.2 性能监控
建议添加执行时间和资源使用情况的监控。

---

*此文档描述了RMDB查询优化器的核心设计和实现。随着系统的演进，优化策略和算法可能会持续改进和扩展。*