%{

#include "ast.h"
#include "yacc.tab.h"
#include <iostream>
#include <memory>

// 词法分析器接口函数声明
int yylex(YYSTYPE *yylval, YYLTYPE *yylloc, void *yyscanner);

// 语法错误处理函数
void yyerror(YYLTYPE *locp, void *yyscanner, const char* s) {
    std::cerr << "Parser Error at line " << locp->first_line << " column " << locp->first_column << ": " << s << std::endl;
}

using namespace ast;
%}

// 生成纯净（可重入）的解析器，支持多线程
%define api.pure full
// 启用位置追踪功能，用于错误报告
%locations
// 启用详细的语法错误信息
%define parse.error verbose

%param {void *yyscanner}


// SQL关键字 - 这些是保留字，在词法分析阶段识别
%token SHOW TABLES CREATE TABLE DROP DESC INSERT INTO VALUES DELETE FROM ASC ORDER GROUP BY HAVING COUNT SUM AVG MIN MAX
%token WHERE UPDATE SET SELECT INT CHAR FLOAT DATETIME INDEX AND JOIN EXIT HELP TXN_BEGIN TXN_COMMIT TXN_ABORT TXN_ROLLBACK ENABLE_NESTLOOP ENABLE_SORTMERGE
%token LIMIT OFFSET
%token EXPLAIN AS
%token INNER_JOIN LEFT_JOIN RIGHT_JOIN FULL_JOIN ON SEMI OFF
%token STATIC_CHECKPOINT
%token LOAD OUTPUT_FILE

// 复合操作符 - 由多个字符组成的操作符
%token LEQ NEQ GEQ T_EOF

// 带语义值的Token - 这些Token携带具体的数据
%token <sv_str> IDENTIFIER VALUE_STRING VALUE_PATH    // 标识符和字符串字面量
%token <sv_int> VALUE_INT                             // 整数字面量
%token <sv_float> VALUE_FLOAT                         // 浮点数字面量
%token <sv_bool> VALUE_BOOL                           // 布尔字面量


// 语句类型 - 所有返回TreeNode的语法规则
%left '+' '-'
%left '*' '/'

// 语句类型 - 所有返回TreeNode的语法规则
%type <sv_node> stmt dbStmt ddl dml txnStmt setStmt loadStmt setOutputStmt
%type <sv_limit> opt_limit_clause

// 表结构相关
%type <sv_field> field                      // 单个字段定义
%type <sv_fields> fieldList                 // 字段列表

// 数据类型
%type <sv_type_len> type                    // 数据类型定义

// 表达式和操作符
%type <sv_comp_op> op                       // 比较操作符
%type <sv_expr> expr term factor            // 表达式 (包括算术表达式)
%type <sv_val> value                        // 值
%type <sv_vals> valueList                   // 值列表

// 标识符
%type <sv_str> tbName colName filePath              // 表名和列名

// 列表类型
%type <sv_strs> colNameList                 // 列名列表
%type <sv_col> col                          // 列引用
%type <sv_cols> colList selector            // 列列表和选择器
%type <sv_table_ref> tableRef               // 表引用（支持别名）
%type <sv_table_refs> tableList             // 表引用列表
%type <sv_str> optAlias                     // 可选别名规则

// UPDATE相关
%type <sv_set_clause> setClause             // SET子句
%type <sv_set_clauses> setClauses           // SET子句列表

// WHERE相关
%type <sv_cond> condition                   // 条件表达式
%type <sv_conds> whereClause optWhereClause opt_on_clause // WHERE子句

// ORDER BY相关
%type <sv_orderby> order_clause 
%type <sv_orderbys> order_clauses opt_order_clause  // ORDER BY子句
%type <sv_orderby_dir> opt_asc_desc                 // 排序方向

// JOIN相关
%type <sv_join_type> joinType                // JOIN类型
%type <sv_join_expr> joinExpr                // JOIN表达式
%type <sv_join_exprs> joinExprs optJoinExprs // JOIN表达式列表

// group相关
%type <sv_groupby> group_clause
%type <sv_groupbys> group_clauses opt_group_clause
%type <sv_having_conds> having_conds opt_having_conds

// 配置相关
%type <sv_setKnobType> set_knob_type        // 配置选项类型

%%

/* 解析入口点 */
start:
        stmt ';'                            // 标准SQL语句（以分号结尾）
    {
        parse_tree = $1;                    // 设置解析结果
        YYACCEPT;                           // 成功完成解析
    }
    |   HELP                                // HELP命令
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
    |   EXIT                                // EXIT命令
    {
        parse_tree = nullptr;               // 空树表示退出
        YYACCEPT;
    }
    |   T_EOF                               // 文件结束
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |   setOutputStmt                       // 设置输出文件
    {
        parse_tree = $1;                    // 设置解析结果为输出设置语句
        YYACCEPT;
    }
    ;

/* 语句分类 - SQL语句的顶层分类 */
stmt:
        dbStmt                              // 数据库管理语句
    |   ddl                                 // 数据定义语言（DDL）
    |   dml                                 // 数据操作语言（DML）
    |   txnStmt                             // 事务控制语句
    |   setStmt                             // 配置设置语句
    |   loadStmt                            // LOAD命令
    ;

/* 事务控制语句 */
txnStmt:
        TXN_BEGIN                           // 开始事务
    {
        $$ = std::make_shared<TxnBegin>();
    }
    |   TXN_COMMIT                          // 提交事务
    {
        $$ = std::make_shared<TxnCommit>();
    }
    |   TXN_ABORT                           // 中止事务
    {
        $$ = std::make_shared<TxnAbort>();
    }
    | TXN_ROLLBACK                          // 回滚事务
    {
        $$ = std::make_shared<TxnRollback>();
    }
    ;

/* 数据库管理语句 */
dbStmt:
        SHOW TABLES                         // 显示所有表
    {
        $$ = std::make_shared<ShowTables>();
    }
    |   SHOW INDEX FROM tbName              // 显示指定表的索引
    {
        $$ = std::make_shared<ShowIndex>($4);
    }
    ;

/* 配置设置语句 */
setStmt:
        SET set_knob_type '=' VALUE_BOOL    // 设置配置选项
    {
        $$ = std::make_shared<SetStmt>($2, $4);
    }
    ;

/* LOAD命令 */
loadStmt:
        LOAD filePath INTO tbName
    {
        $$ = std::make_shared<LoadStmt>($2, $4);
    }
    ;

/* SET OUTPUT_FILE命令 */
setOutputStmt:
        SET OUTPUT_FILE ON
    {
        $$ = std::make_shared<SetOutputStmt>(true);
    }
    |   SET OUTPUT_FILE OFF
    {
        $$ = std::make_shared<SetOutputStmt>(false);
    }
    ;

/* DDL - 数据定义语言 */
ddl:
        CREATE TABLE tbName '(' fieldList ')'  // 创建表
    {
        $$ = std::make_shared<CreateTable>($3, $5);
    }
    |   DROP TABLE tbName                   // 删除表
    {
        $$ = std::make_shared<DropTable>($3);
    }
    |   DESC tbName                         // 描述表结构
    {
        $$ = std::make_shared<DescTable>($2);
    }
    |   CREATE INDEX tbName '(' colNameList ')'  // 创建索引
    {
        $$ = std::make_shared<CreateIndex>($3, $5);
    }
    |   DROP INDEX tbName '(' colNameList ')'    // 删除索引
    {
        $$ = std::make_shared<DropIndex>($3, $5);
    }
    |   CREATE STATIC_CHECKPOINT
    {
        $$ = std::make_shared<CreateStaticCheckpoint>();
    }
    ;

/* DML - 数据操作语言 */
dml:
        INSERT INTO tbName VALUES '(' valueList ')'  // 插入数据
    {
        $$ = std::make_shared<InsertStmt>($3, $6);
    }
    |   DELETE FROM tbName optWhereClause   // 删除数据
    {
        $$ = std::make_shared<DeleteStmt>($3, $4);
    }
    |   UPDATE tableRef SET setClauses optWhereClause  // 更新数据
    {
        $$ = std::make_shared<UpdateStmt>($2, $4, $5);
    }
    |   SELECT selector FROM tableList optJoinExprs optWhereClause opt_group_clause opt_having_conds opt_order_clause opt_limit_clause  // 查询数据(支持表别名，列别名，JOIN)
    {
        $$ = std::make_shared<SelectStmt>($2, $4, $5, $6, $7, $8, $9, $10);
    }
    |   EXPLAIN SELECT selector FROM tableList optJoinExprs optWhereClause opt_group_clause opt_having_conds opt_order_clause opt_limit_clause  // 查询数据(支持表别名，列别名，JOIN)
    {
        $$ = std::make_shared<ExplainStmt>($3, $5, $6, $7, $8, $9, $10, $11);
    };

/* LIMIT子句 */
opt_limit_clause:
        /* epsilon */
    {
        $$ = nullptr;
    }
    |   LIMIT value
    {
        $$ = std::make_shared<Limit>(nullptr, $2);
    }
    |   LIMIT value OFFSET value
    {
        $$ = std::make_shared<Limit>($4, $2);
    };

/* 字段列表 - 用于CREATE TABLE */
fieldList:
        field                               // 单个字段
    {
        $$ = std::vector<std::shared_ptr<Field>>{$1};
    }
    |   fieldList ',' field                 // 多个字段（递归定义）
    {
        $$.push_back($3);                   // 向现有列表添加新字段
    }
    ;

/* 列名列表 - 用于索引定义等 */
colNameList:
        colName                             // 单个列名
    {
        $$ = std::vector<std::string>{$1};
    }
    | colNameList ',' colName               // 多个列名（递归定义）
    {
        $$.push_back($3);                   // 向现有列表添加新列名
    }
    ;

/* 字段定义 - 列名和数据类型的组合 */
field:
        colName type                        // 列名 + 数据类型
    {
        $$ = std::make_shared<ColDef>($1, $2);
    }
    ;

/* 数据类型定义 */
type:
        INT                                 // 整数类型
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
    |   CHAR '(' VALUE_INT ')'              // 字符串类型（指定长度）
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, $3);
    }
    |   FLOAT                               // 浮点数类型
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
    |   DATETIME                            // 日期时间类型
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, 19);
    }
    ;

/* 值列表 - 用于INSERT语句 */
valueList:
        value                               // 单个值
    {
        $$ = std::vector<std::shared_ptr<Value>>{$1};
    }
    |   valueList ',' value                 // 多个值（递归定义）
    {
        $$.push_back($3);                   // 向现有列表添加新值
    }
    ;

/* 值定义 - 字面量常量 */
value:
        VALUE_INT                           // 整数字面量
    {
        $$ = std::make_shared<IntLit>($1);
    }
    |   VALUE_FLOAT                         // 浮点数字面量
    {
        $$ = std::make_shared<FloatLit>($1);
    }
    |   VALUE_STRING                        // 字符串字面量
    {
        $$ = std::make_shared<StringLit>($1);
    }
    |   VALUE_BOOL                          // 布尔字面量
    {
        $$ = std::make_shared<BoolLit>($1);
    }
    ;

/* 条件表达式 - 用于WHERE子句 */
condition:
        col op expr                         // 列 操作符 表达式
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $3);
    }
    ;

/* 可选的WHERE子句 */
optWhereClause:
        /* epsilon */                       // 空规则 - 没有WHERE子句
    { 
        /* 不做任何操作，保持默认值 */ 
    }
    |   WHERE whereClause                   // 有WHERE子句
    {
        $$ = $2;                            // 传递WHERE子句的内容
    }
    ;

/* WHERE子句内容 */
whereClause:
        condition                           // 单个条件
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    |   whereClause AND condition           // 多个条件用AND连接
    {
        $$.push_back($3);                   // 向条件列表添加新条件
    }
    ;

/* 可选别名规则 */
optAlias:
        /* epsilon */                       // 没有别名
    {
        $$ = "";                            // 空字符串表示没有别名
    }
    |   AS IDENTIFIER                       // AS 别名
    {
        $$ = $2;                            // 返回别名
    }
    |   IDENTIFIER                          // 直接跟别名（省略AS）
    {
        $$ = $1;                            // 返回别名
    }
    ;

/* 列引用 */
col:
        tbName '.' colName optAlias         // 表名.列名 [AS 别名]
    {
        $$ = std::make_shared<Col>($1, $3, $4);
    }
    |   colName optAlias                    // 列名 [AS 别名]
    {
        $$ = std::make_shared<Col>("", $1, $2); // 表名为空字符串
    }
    |   COUNT '(' '*' ')' optAlias        // COUNT(*) [AS 别名]
    {
        $$ = std::make_shared<Col>("", "*", $5, SvAggregateType::COUNT); // 特殊处理COUNT(*)
    }
    |   COUNT '(' colName ')' optAlias  // COUNT(列名) [AS 别名]
    {
        $$ = std::make_shared<Col>("", $3, $5, SvAggregateType::COUNT);
    }
    |   COUNT '(' tbName '.' colName ')' optAlias  // COUNT(表名.列名) [AS 别名]
    {
        $$ = std::make_shared<Col>($3, $5, $7, SvAggregateType::COUNT);
    }
    |   SUM '(' colName ')' optAlias       // SUM(列名) [AS 别名]
    {
        $$ = std::make_shared<Col>("", $3, $5, SvAggregateType::SUM);
    }
    |   SUM '(' tbName '.' colName ')' optAlias  // SUM(表名.列名) [AS 别名]
    {
        $$ = std::make_shared<Col>($3, $5, $7, SvAggregateType::SUM);
    }
    |   AVG '(' colName ')' optAlias       // AVG(列名) [AS 别名]
    {
        $$ = std::make_shared<Col>("", $3, $5, SvAggregateType::AVG);
    }
    |   AVG '(' tbName '.' colName ')' optAlias  // AVG(表名.列名) [AS 别名]
    {
        $$ = std::make_shared<Col>($3, $5, $7, SvAggregateType::AVG);
    }
    |   MIN '(' colName ')' optAlias       // MIN(列名) [AS 别名]
    {
        $$ = std::make_shared<Col>("", $3, $5, SvAggregateType::MIN);
    }
    |   MIN '(' tbName '.' colName ')' optAlias  // MIN(表名.列名) [AS 别名]
    {
        $$ = std::make_shared<Col>($3, $5, $7, SvAggregateType::MIN);
    }
    |   MAX '(' colName ')' optAlias       // MAX(列名) [AS 别名]
    {
        $$ = std::make_shared<Col>("", $3, $5, SvAggregateType::MAX);
    }
    |   MAX '(' tbName '.' colName ')' optAlias  // MAX(表名.列名) [AS 别名]
    {
        $$ = std::make_shared<Col>($3, $5, $7, SvAggregateType::MAX);
    }
    ;

/* 列列表 - 用于SELECT等 */
colList:
        col                                 // 单个列
    {
        $$ = std::vector<std::shared_ptr<Col>>{$1};
    }
    |   colList ',' col                     // 多个列（递归定义）
    {
        $$.push_back($3);                   // 向列表添加新列
    }
    ;

/* 比较操作符 */
op:
        '='                                 // 等于
    {
        $$ = SV_OP_EQ;
    }
    |   '<'                                 // 小于
    {
        $$ = SV_OP_LT;
    }
    |   '>'                                 // 大于
    {
        $$ = SV_OP_GT;
    }
    |   NEQ                                 // 不等于 (!=)
    {
        $$ = SV_OP_NE;
    }
    |   LEQ                                 // 小于等于 (<=)
    {
        $$ = SV_OP_LE;
    }
    |   GEQ                                 // 大于等于 (>=)
    {
        $$ = SV_OP_GE;
    }
    ;

// 表达式 - 可以是值、列引用或算术表达式
expr:
        term
    |   expr '+' term
    {
        // 需要在 ast.h 中定义 ArithExpr 和 SV_ARITH_PLUS
        $$ = std::make_shared<ArithExpr>($1, SV_ARITH_PLUS, $3);
    }
    |   expr '-' term
    {
        // 需要在 ast.h 中定义 SV_ARITH_MINUS
        $$ = std::make_shared<ArithExpr>($1, SV_ARITH_MINUS, $3);
    }
    ;

term:
        factor
    |   term '*' factor
    {
        // 需要在 ast.h 中定义 SV_ARITH_MULTIPLY
        $$ = std::make_shared<ArithExpr>($1, SV_ARITH_MULTIPLY, $3);
    }
    |   term '/' factor
    {
        // 需要在 ast.h 中定义 SV_ARITH_DIVIDE
        $$ = std::make_shared<ArithExpr>($1, SV_ARITH_DIVIDE, $3);
    }
    ;

factor:
        value                               // 值表达式
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col                                 // 列引用表达式
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   '(' expr ')'                      // 括号表达式
    {
        $$ = $2; // Pass through the inner expression
    }
    /* |   '+' factor
    {
        $$ = $2; // 一元加不改变数值，直接返回操作数
    }
    |   '-' factor
    {
        // 创建一个表示零的 IntLit 节点作为左操作数
        auto zero_lit = std::make_shared<IntLit>(0);
        // 创建一个 ArithExpr 节点表示 0 - factor
        $$ = std::make_shared<ArithExpr>(zero_lit, SV_ARITH_MINUS, $2);
    } */
    ;

/* SET子句列表 - 用于UPDATE语句 */
setClauses:
        setClause                           // 单个SET子句
    {
        $$ = std::vector<std::shared_ptr<SetClause>>{$1};
    }
    |   setClauses ',' setClause            // 多个SET子句
    {
        $$.push_back($3);                   // 向列表添加新的SET子句
    }
    ;

// SET子句 - 列名=表达式
setClause:
        colName '=' expr                   // 列名 = 表达式 (可以是值、列或算术表达式)
    {
        // 需要修改 ast.h 中的 SetClause 以接受 Expr 而不是 Value
        $$ = std::make_shared<SetClause>($1, $3);
    }
    ;

/* 选择器 - SELECT语句中的列选择 */
selector:
        '*'                                 // 选择所有列
    {
        $$ = {};                            // 空向量表示选择所有列
    }
    |   colList                             // 选择指定列
    ;

/* 表引用 - 支持别名 */
tableRef:
        tbName optAlias                     // 表名 [AS 别名]
    {
        
            $$ = std::make_shared<TableRef>($1, $2);
        
    }
    ;

/* 表列表 - FROM子句中的表 */
tableList:
        tableRef                            // 单个表引用
    {
        $$ = std::vector<std::shared_ptr<TableRef>>{$1};
    }
    |   tableList ',' tableRef              // 多个表（逗号分隔）
    {
        $$.push_back($3);                   // 向表列表添加新表
    }
    ;

/* 可选的ORDER BY子句 */
opt_order_clause:
    ORDER BY order_clauses                   // 有ORDER BY子句
    { 
        $$ = $3;
    }
    |   /* epsilon */                       // 没有ORDER BY子句
    { 
        /* 不做任何操作，保持默认值 */ 
    }
    ;

/* ORDER BY子句内容 */
order_clause:
      col opt_asc_desc                      // 列名 + 可选的排序方向
    { 
        $$ = std::make_shared<OrderBy>($1, $2);
    }
    ;

order_clauses:
      order_clause                          // 单个ORDER BY子句
    {
        $$ = std::vector<std::shared_ptr<OrderBy>>{$1};
    }
    |   order_clauses ',' order_clause      // 多个ORDER BY子句
    {
        $$.push_back($3);                   // 向现有列表添加新ORDER BY子句
    }

/* 可选的排序方向 */
opt_asc_desc:
    ASC                                     // 升序
    { 
        $$ = OrderBy_ASC; 
    }
    |  DESC                                 // 降序
    { 
        $$ = OrderBy_DESC; 
    }
    |                                       // 默认（通常是升序）
    { 
        $$ = OrderBy_DEFAULT; 
    }
    ;

opt_group_clause:
    GROUP BY group_clauses
    {
        $$ = $3;
    }
    |   /* epsilon */ { /* ignore*/ }
    ;
group_clause:
      col
    {
        $$ = std::make_shared<GroupBy>($1);
    }
group_clauses:
      group_clause
    {
        $$ = std::vector<std::shared_ptr<GroupBy>>{$1};
    }
    |	group_clauses ',' group_clause
    {
        $$.push_back($3);
    }
    ;
opt_having_conds:
       /* epsilon */ { /* ignore*/ }
    |   HAVING having_conds
    {
        $$ = $2;
    }
    ;
having_conds:
        condition 
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    |   having_conds AND condition
    {
        $$.push_back($3);
    }
    ;


/* 可选的JOIN表达式列表 */
optJoinExprs:
        /* epsilon */                       // 没有JOIN
    {
        $$ = std::vector<std::shared_ptr<JoinExpr>>{};
    }
    |   joinExprs                          // 有JOIN表达式
    {
        $$ = $1;
    }
    ;

/* JOIN表达式列表 */
joinExprs:
        joinExpr                           // 单个JOIN表达式
    {
        $$ = std::vector<std::shared_ptr<JoinExpr>>{$1};
    }
    |   joinExprs joinExpr                 // 多个JOIN表达式
    {
        $$.push_back($2);
    }
    ;

opt_on_clause:
        ON whereClause                      // 有ON条件
    {
        $$ = $2;                            // 返回WHERE子句内容
    }
    |   /* epsilon */                       // 没有ON条件
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{};
    }
    ;

/* JOIN表达式 */
joinExpr:
        joinType tableRef opt_on_clause   // JOIN类型 表 ON 条件
    {
        $$ = std::make_shared<JoinExpr>($2, $3, $1);
    }
    ;

/* JOIN类型 */
joinType:
        JOIN                               // 默认INNER JOIN
    {
        $$ = SV_INNER_JOIN;
    }
    |   INNER_JOIN                         // 显式INNER JOIN
    {
        $$ = SV_INNER_JOIN;
    }
    |   LEFT_JOIN                          // LEFT JOIN
    {
        $$ = SV_LEFT_JOIN;
    }
    |   RIGHT_JOIN                         // RIGHT JOIN
    {
        $$ = SV_RIGHT_JOIN;
    }
    |   FULL_JOIN                          // FULL JOIN
    {
        $$ = SV_FULL_JOIN;
    }
    |   SEMI JOIN                              // SEMI JOIN
    {
        $$ = SV_SEMI_JOIN;
    }
    ;

/* 配置选项类型 */
set_knob_type:
    ENABLE_NESTLOOP                         // 启用嵌套循环连接
    {
        $$ = EnableNestLoop;
    }
    |   ENABLE_SORTMERGE                    // 启用排序合并连接
    {
        $$ = EnableSortMerge;
    }
    ;

/* 基本标识符规则 */
tbName: IDENTIFIER;                         // 表名就是标识符
colName: IDENTIFIER;                        // 列名就是标识符
filePath: VALUE_PATH;                     // 文件路径就是字符串字面量
%%
