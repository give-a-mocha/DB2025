#!/bin/bash

# 检查客户端程序是否存在
if [ ! -f "rmdb_client/build/rmdb_client" ]; then
    echo "错误: rmdb_client 不存在，请先编译项目"
    exit 1
fi

# 检查服务端程序是否存在
if [ ! -f "build/bin/rmdb" ]; then
    echo "错误: rmdb 服务端不存在，请先编译项目"
    exit 1
fi

# 启动服务端
echo "正在启动数据库服务端..."
orb ./test.sh
tableName="index_test_db"

# 检查并删除表名对应的文件夹
if [ -d "$tableName" ]; then
    echo "发现文件夹 $tableName,正在删除..."
    rm -rf "$tableName"
    echo "文件夹 $tableName 已删除"
else
    echo "文件夹 $tableName 不存在"
fi

orb build/bin/rmdb $tableName &
SERVER_PID=$!
echo "服务端已启动,PID: $SERVER_PID"

# 等待服务端启动完成
sleep 2

# 创建测试SQL文件
cat > test_commands.sql << EOF
create table warehouse (id int, name char(8));
create index warehouse (id);
show index from warehouse;
create index warehouse (id,name);
show index from warehouse;
drop index warehouse (id);
drop index warehouse (id,name);
show index from warehouse;
exit;
EOF

echo "正在启动数据库客户端并执行测试命令..."
echo "=========================================="

# 运行客户端并传入测试命令
orb rmdb_client/build/rmdb_client < test_commands.sql

echo "=========================================="
echo "客户端测试完成"

# 停止服务端进程
echo "正在停止数据库服务端..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
echo "服务端已停止"

# 清理临时文件
rm -f test_commands.sql
