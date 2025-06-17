#!/bin/bash

# 设置颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 帮助信息
show_help() {
    echo -e "${YELLOW}用法: $0 [选项]${NC}"
    echo "选项:"
    echo "  -h, --help     显示帮助信息"
    echo "  -c, --clean    清理构建目录"
    echo
}

# 错误处理函数
error_exit() {
    echo -e "${RED}错误: $1${NC}" 1>&2
    exit 1
}

# 清理函数
clean_build() {
    echo -e "${YELLOW}清理构建目录...${NC}"
    rm -rf ./build
    echo -e "${GREEN}清理完成${NC}"
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
sleep 1

# --- 6. 编译 Client ---
echo "\n🛠️ 正在编译 client..."
mkdir -p rmdb_client/build
(
    cd rmdb_client/build
    cmake .. > /dev/null
    make rmdb_client -j$(nproc)
)
echo "✅ client 编译完成！"

# 运行test_parser
echo -e "${YELLOW}运行test_parser...${NC}"
./bin/test_parser || error_exit "test_parser执行失败"
echo -e "${GREEN}test_parser执行成功${NC}"