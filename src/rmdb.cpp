/**
 * @file rmdb.cpp
 * @author RMDB Development Team
 * @brief RMDB服务器主程序入口
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * RMDB is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#include <atomic>
#include <cstdio>

#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>

#include "analyze/analyze.h"
#include "common/TraceStack.hpp"
#include "common/print.hpp"
#include "errors.h"
#include "optimizer/optimizer.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "recovery/log_recovery.h"
#include "common/utils/Format.h"
#include "common/getFunctionTime.h"

constexpr int SOCK_PORT = 8765;
constexpr int MAX_CONN_LIMIT = 16;
constexpr bool ENABLE_COUT = true;
constexpr bool ENABLE_TIMER = false;
static bool should_exit = false;
// #define ENABLE_SERIALIZE

// 构建全局所需的管理器对象
DiskManager disk_manager;
BufferPoolManager buffer_pool_manager;
RmManager rm_manager;
IxManager ix_manager;
SmManager sm_manager;
LockManager lock_manager;
TransactionManager txn_manager(ConcurrencyMode::MVCC);
Planner planner;
Optimizer optimizer;
LogManager log_manager;
RecoveryManager recovery;
QlManager ql_manager;
Portal portal;
Analyze analyze;

// pthread_mutex_t *buffer_mutex;
pthread_mutex_t *sockfd_mutex;

#ifdef ENABLE_SERIALIZE
pthread_mutex_t *sql_mutex;
#endif

template <bool Flush = false, typename... Args>
void Print(std::string_view fmt_str, Args &&...args) {
    if constexpr (ENABLE_COUT) {
        std::cout << util::format(std::string(fmt_str), std::forward<Args>(args)...);
        if constexpr (Flush) {
            std::cout.flush();
        }
    }
}

/* 用于处理Ctrl+C信号的跳转缓冲区 */
static jmp_buf jmpbuf;

/**
 * @brief 处理Ctrl+C信号的处理函数
 * @param signo 信号编号
 * @note 当接收到SIGINT信号时，将日志刷新到磁盘并优雅地退出服务器
 */
void sigint_handler(int signo) {
    TRACE_FUNCTION
    should_exit = true;
    // log_manager->flush_log_to_disk();
    Print("The Server receive Crtl+C, will been closed\n");
    if constexpr (ENABLE_TIMER) {
        PrintFunctionTime
    }
    longjmp(jmpbuf, 1);
}

/**
 * @brief 设置当前事务上下文
 * @param txn_id 事务ID指针
 * @param context 数据库上下文
 * @note 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
 *       如果当前没有活跃事务或事务已结束，则创建新的事务
 */
void SetTransaction(txn_id_t *txn_id, Context *context) {
    TRACE_FUNCTION
    // 获取事务对象
    if (txn_manager.exsit_transaction(*txn_id)) {
        // 如果事务ID存在，则获取对应的事务对象
        context->txn_ = txn_manager.get_transaction(*txn_id);
    } else {
        context->txn_ = nullptr;
    }
    // 如果事务对象为空 或者已提交 或者已中止， 则创建新事务
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager.begin(nullptr);
        *txn_id = context->txn_->get_transaction_id();
    }
}

/**
 * @brief 处理客户端连接的线程函数
 * @param sock_fd 客户端socket文件描述符
 * @return void*
 * @note 负责接收和处理来自客户端的SQL请求：
 *       1. 接收客户端发送的SQL语句
 *       2. 解析SQL语句(Parser)
 *       3. 分析和重写查询(Analyzer)
 *       4. 优化查询计划(Optimizer)
 *       5. 执行查询(Executor)
 *       6. 返回结果给客户端
 */
void *client_handler(void *sock_fd) {
    TRACE_FUNCTION
    int fd = *((int *)sock_fd);
    pthread_mutex_unlock(sockfd_mutex);

    int i_recvBytes;
    // 接收客户端发送的请求
    char data_recv[BUFFER_LENGTH];
    // 需要返回给客户端的结果
    char data_send[BUFFER_LENGTH];
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    txn_id_t txn_id = INVALID_TXN_ID;
    // 初始化parser
    yyscan_t scanner;
    yylex_init(&scanner);

    Print("establish client connection, sockfd: {}\n", fd);

    while (true) {
        Print<true>("Waiting for request...\n");

        i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);
        data_recv[i_recvBytes] = '\0';

        if (i_recvBytes == 0) {
            Print<true>("Maybe the client has closed\n");
            break;
        }
        if (i_recvBytes == -1) {
            Print<true>("Client read error!\n");
            break;
        }

        Print("i_recvBytes: {} \n ", i_recvBytes);

        if (strncasecmp(data_recv, "exit", 4) == 0) {
            Print("Client exit.\n", true);
            break;
        }
        if (strncasecmp(data_recv, "crash", 5) == 0) {
            Print("Server crash.\n", true);
            exit(1);
        }

        Print<true>("Read from client {}: {}\n", fd, data_recv);

#ifdef ENABLE_SERIALIZE
        pthread_mutex_lock(sql_mutex);
#endif
        offset = 0;

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        auto context = std::make_unique<Context>(nullptr, data_send, &offset);
        SetTransaction(&txn_id, context.get());

        // 用于判断是否已经调用了yy_delete_buffer来删除buf
        bool finish_analyze = false;
        YY_BUFFER_STATE buf = yy_scan_string(data_recv, scanner);
        if (yyparse(scanner) == 0) {
            if (ast::parse_tree != nullptr) {
                try {
                    std::shared_ptr<Query> query = analyze.do_analyze(std::move(ast::parse_tree));
                    yy_delete_buffer(buf, scanner);
                    finish_analyze = true;
                    std::shared_ptr<Plan> plan = optimizer.plan_query(query, context.get());
                    std::shared_ptr<PortalStmt> portalStmt = portal.start(plan, context.get());
                    portal.run(portalStmt, &txn_id, context.get());
                    portal.drop();
                } catch (TransactionAbortException &e) {
                    memcpy(data_send, "abort\n", 6);
                    data_send[6] = '\0';
                    offset = 6;

                    std::this_thread::sleep_for(std::chrono::milliseconds(3));

                    // 回滚事务
                    txn_manager.abort(context.get());
                    // if (txn_manager->should_perform_gc()) {
                    //     // 如果事务数量过多，或者有大量已终止的事务，则执行垃圾回收
                    //     txn_manager->GarbageCollection();
                    // }
                    Print<true>("{}\n", e.GetInfo());

                    if (sm_manager.is_output_file_) {
                        std::fstream outfile;
                        outfile.open("output.txt", std::ios::out | std::ios::app);
                        outfile << "abort\n";
                        outfile.close();
                    }
                } catch (RMDBError &e) {
                    // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
                    // 遇到异常，打印异常信息
                    Print<true>("RMDBError {}\n", e.what());

                    memcpy(data_send, e.what(), e.get_msg_len());
                    data_send[e.get_msg_len()] = '\n';
                    data_send[e.get_msg_len() + 1] = '\0';
                    offset = e.get_msg_len() + 1;

                    if (sm_manager.is_output_file_) {
                        // 将报错信息写入output.txt
                        std::fstream outfile;
                        outfile.open("output.txt", std::ios::out | std::ios::app);
                        outfile << "failure\n";
                        outfile.close();
                    }
                }
            }
        } else {
            // 语法分析失败，返回错误信息
            constexpr const char *str = "syntax error\n";
            constexpr size_t len = 14;
            memcpy(data_send, str, len);
            data_send[len] = '\0';
            offset = len;

            if (sm_manager.is_output_file_) {
                // 将报错信息写入output.txt
                std::fstream outfile;
                outfile.open("output.txt", std::ios::out | std::ios::app);
                outfile << "failure\n";
                outfile.close();
            }
        }
        if (finish_analyze == false) {
            yy_delete_buffer(buf, scanner);
            // pthread_mutex_unlock(buffer_mutex);
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if (write(fd, data_send, offset + 1) == -1) {
#ifdef ENABLE_SERIALIZE
            pthread_mutex_unlock(sql_mutex);
#endif
            break;
        }
        // 如果是单挑语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
        if (context->txn_ != nullptr && context->txn_->get_txn_mode() == false) {
            txn_manager.commit(context->txn_);
            // if (txn_manager->should_perform_gc()) {
            //     // 如果事务数量过多，或者有大量已终止的事务，则执行垃圾回收
            //     txn_manager->GarbageCollection();
            // }
        }

#ifdef ENABLE_SERIALIZE
        pthread_mutex_unlock(sql_mutex);
#endif
    }

    yylex_destroy(scanner);  // 销毁扫描器

    // Clear
    Print<true>("Terminating current client_connection...\n");
    close(fd);           // close a file descriptor.
    pthread_exit(NULL);  // terminate calling thread!
}

/**
 * @brief 启动RMDB服务器
 * @note 主要功能：
 *       1. 初始化服务器socket和互斥锁
 *       2. 监听客户端连接请求
 *       3. 对每个客户端连接创建新的处理线程
 *       4. 处理服务器关闭时的清理工作
 */
void start_server() {
    TRACE_FUNCTION
    // init mutex
    // buffer_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    sockfd_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    // pthread_mutex_init(buffer_mutex, nullptr);
    pthread_mutex_init(sockfd_mutex, nullptr);

#ifdef ENABLE_SERIALIZE
    sql_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(sql_mutex, nullptr);
#endif

    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in {};

    // 初始化连接
    sockfd_server = socket(AF_INET, SOCK_STREAM, 0);  // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(SOCK_PORT);
    fd_temp = bind(sockfd_server, (struct sockaddr *)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1) {
        Print<true>("Bind error!\n");
        exit(1);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1) {
        Print<true>("Listen error!\n");
        exit(1);
    }

    while (!should_exit) {
        Print<true>("Waiting for new connection...\n");
        pthread_t thread_id;

        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);
        if (setjmp(jmpbuf)) {
            Print("Break from Server Listen Loop\n");
            break;
        }

        // Block here. Until server accepts a new connection.
        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
        if (sockfd == -1) {
            Print<true>("Accept error!\n");
            continue;  // ignore current socket ,continue while loop.
        }

        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        if (pthread_create(&thread_id, nullptr, &client_handler, (void *)(&sockfd)) != 0) {
            Print<true>("Create thread fail!\n");
            break;  // break while loop
        }
    }

    // Clear
    Print(" Try to close all client-connection.\n");

    int ret = shutdown(sockfd_server, SHUT_WR);
    // shut down the all or part of a full-duplex connection.
    if (ret == -1) {
        printf("%s\n", strerror(errno));
    }
    //    assert(ret != -1);
    sm_manager.close_db();
    Print(" DB has been closed.\n");
    Print<true>("Server shuts down.\n");
}

/**
 * @brief RMDB主程序入口
 * @param argc 参数个数
 * @param argv 参数值数组，argv[1]为数据库名称
 * @return int 返回程序执行状态
 * @note 主要功能：
 *       1. 初始化数据库环境
 *       2. 执行数据库恢复流程
 *       3. 启动服务器接受客户端连接
 */
int main(int argc, char **argv) {
    TRACE_FUNCTION
    if (argc != 2) {
        // 需要指定数据库名称
        std::cerr << "Usage: " << argv[0] << " <database>" << std::endl;
        exit(1);
    }

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);
    signal(SIGQUIT, sigint_handler);
    signal(SIGHUP, sigint_handler);
    signal(SIGUSR1, sigint_handler);
    signal(SIGUSR2, sigint_handler);
    try {
        Print(
            "\n"
            "  _____  __  __ _____  ____  \n"
            " |  __ \\|  \\/  |  __ \\|  _ \\ \n"
            " | |__) | \\  / | |  | | |_) |\n"
            " |  _  /| |\\/| | |  | |  _ < \n"
            " | | \\ \\| |  | | |__| | |_) |\n"
            " |_|  \\_\\_|  |_|_____/|____/ \n"
            "\n"
            "Welcome to RMDB!\n"
            "Type 'help;' for help.\n"
            "\n");

        // Database name is passed by args
        std::string db_name = argv[1];
        if (!sm_manager.is_dir(db_name)) {
            // Database not found, create a new one
            sm_manager.create_db(db_name);
        }
        // Open database
        sm_manager.open_db(db_name);

        // recovery database
        // recovery->recovery();

        // 开启服务端，开始接受客户端连接
        start_server();
    } catch (RMDBError &e) {
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    return 0;
}
