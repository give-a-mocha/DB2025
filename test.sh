#!/bin/zsh

# 当任何命令执行失败时，立即退出脚本
set -e

# --- 1. 参数检查与帮助信息 ---
# 定义帮助信息函数
print_usage() {
    echo "用法: $0 [sql_file_path]"
    echo
    echo "运行 RMDB 测试脚本。"
    echo
    echo "参数:"
    echo "  sql_file_path    可选。要执行的 SQL 文件的路径。"
    echo "                   如果未提供，则默认为 'test.sql'。"
    echo
    echo "选项:"
    echo "  --help, -h       显示此帮助信息并退出。"
}

# 检查帮助选项
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    print_usage
    exit 0
fi

# 检查参数数量
if [ $# -gt 1 ]; then
    echo "❌ 错误: 参数过多。" >&2
    print_usage >&2
    exit 1
fi

# 设置 SQL 文件路径
sql_file=${1:-test.sql}

# 检查文件是否存在
if [ ! -f "$sql_file" ]; then
    echo "❌ 错误: SQL 文件未找到于 '$sql_file'" >&2
    exit 1
fi

# --- 2. 清理函数与陷阱 ---
# 定义一个变量来存储服务器的 PID
server_pid=""

# 定义一个在脚本退出时执行的清理函数
cleanup() {
    echo -e "\n✨ 开始执行清理操作..."
    # -n 检查变量是否非空
    if [ -n "$server_pid" ]; then
        # kill -0 检查进程是否存在
        if kill -0 "$server_pid" > /dev/null 2>&1; then
            echo "🛑 正在停止 server (PID: $server_pid)..."
            kill "$server_pid"
            # 等待进程完全终止，忽略可能出现的 "Terminated" 错误信息
            wait "$server_pid" 2>/dev/null
        else
            echo "🤔 server (PID: $server_pid) 貌似已经不在运行了。"
        fi
    else
        echo "🤷‍ server 进程没有启动，无需停止。"
    fi
    echo "✅ 清理完成！"
}

# 设置一个陷阱（trap），在脚本退出（EXIT）时调用 cleanup 函数
trap cleanup EXIT

# --- 3. 编译 Server ---
echo "\n🛠️ 正在编译 server..."
mkdir -p build
(
    cd build
    cmake .. > /dev/null
    make rmdb -j$(nproc)
)
echo "✅ server 编译完成！"

# --- 4. 清理环境 ---
echo "\n🗑️ 正在清理旧的 output 目录..."
rm -rf build/output
echo "✅ 旧目录清理完成！"

# --- 5. 运行 Server ---
echo "\n🚀 正在后台启动 server..."
# 在子 shell 中执行，避免影响当前目录
(
    cd build
    # 以后台模式启动 server，并将输出重定向到 /dev/null
    ./bin/rmdb output > /dev/null 2>&1 &
)
# 通过进程名获取 server 的 PID
server_pid=$(pgrep rmdb)
echo "✅ server 已启动，PID: $server_pid"
# 等待片刻，确保 server 完全启动
sleep 3

# --- 6. 编译 Client ---
echo "\n🛠️ 正在编译 client..."
mkdir -p rmdb_client/build
(
    cd rmdb_client/build
    cmake .. > /dev/null
    make rmdb_client -j$(nproc)
)
echo "✅ client 编译完成！"

# --- 7. 运行 Client (带计时) ---
echo "\n▶️ 正在使用 SQL 文件 ($sql_file) 运行 client 并计时..."
time rmdb_client/build/rmdb_client < "$sql_file"
echo "✅ client 执行完毕。"

# 脚本正常执行到这里后，会触发 EXIT 陷阱，自动调用 cleanup 函数