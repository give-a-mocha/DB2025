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

#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>

#include "analyze/analyze.h"
#include "common/TraceStack.hpp"
#include "common/print.hpp"
#include "errors.h"
#include "optimizer/optimizer.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "recovery/log_recovery.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 8

static bool should_exit = false;

// 构建全局所需的管理器对象
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager =
    std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto lock_manager = std::make_unique<LockManager>();
auto txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
auto planner = std::make_unique<Planner>(sm_manager.get());
auto optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
auto ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), nullptr);
auto log_manager = std::make_unique<LogManager>(disk_manager.get());
auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get());
auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());
pthread_mutex_t *buffer_mutex;
pthread_mutex_t *sockfd_mutex;

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
    log_manager->flush_log_to_disk();
    std::cout << "The Server receive Crtl+C, will been closed\n";
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
    context->txn_ = txn_manager->get_transaction(*txn_id);
    // 如果事务对象为空 或者已提交 或者已中止， 则创建新事务
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager->begin(nullptr, context->log_mgr_);

        *txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
        // // std::cerr<< "DEBUG: Transaction ID updated to: " << *txn_id <<
        // // std::endl; context->txn_->set_txn_mode(false);
        // // 禁用事务：如果事务为空，则不创建新事务，这会使得后续操作因缺少事务对象而失败或跳过事务相关逻辑
        // // if (context->txn_ == nullptr) {
        // //     std::cerr << "DEBUG: Transaction is null and transaction creation "
        // //                  "is disabled."
        // //               << std::endl;
        // // } else if (context->txn_->get_state() == TransactionState::COMMITTED ||
        // //            context->txn_->get_state() == TransactionState::ABORTED) {
        // //     std::cerr << "DEBUG: Transaction is already committed/aborted and "
        // //                  "transaction creation is disabled."
        // //               << std::endl;
        // // }
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
    char *data_send = new char[BUFFER_LENGTH];
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    txn_id_t txn_id = INVALID_TXN_ID;

    std::string output = "establish client connection, sockfd: " + std::to_string(fd) + "\n";
    std::cout << output;

    while (true) {
        std::cout << "Waiting for request..." << std::endl;
        memset(data_recv, 0, BUFFER_LENGTH);

        i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);

        if (i_recvBytes == 0) {
            std::cout << "Maybe the client has closed" << std::endl;
            break;
        }
        if (i_recvBytes == -1) {
            std::cout << "Client read error!" << std::endl;
            break;
        }

        printf("i_recvBytes: %d \n ", i_recvBytes);

        if (strcmp(data_recv, "exit") == 0) {
            std::cout << "Client exit." << std::endl;
            break;
        }
        if (strcmp(data_recv, "crash") == 0) {
            std::cout << "Server crash" << std::endl;
            exit(1);
        }

        std::cout << "Read from client " << fd << ": " << data_recv << std::endl;

        memset(data_send, '\0', BUFFER_LENGTH);
        offset = 0;

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        auto context = std::make_unique<Context>(lock_manager.get(), log_manager.get(), nullptr, data_send, &offset);
        SetTransaction(&txn_id, context.get());

        // 用于判断是否已经调用了yy_delete_buffer来删除buf
        bool finish_analyze = false;
        pthread_mutex_lock(buffer_mutex);
        YY_BUFFER_STATE buf = yy_scan_string(data_recv);
        if (yyparse() == 0) {
            if (ast::parse_tree != nullptr) {
                try {
                    // analyze and rewrite
                    std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
                    yy_delete_buffer(buf);
                    finish_analyze = true;
                    pthread_mutex_unlock(buffer_mutex);
                    // 优化器
                    std::shared_ptr<Plan> plan = optimizer->plan_query(query, context.get());
                    // portal
                    std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context.get());
                    portal->run(portalStmt, ql_manager.get(), &txn_id, context.get());
                    portal->drop();
                } catch (TransactionAbortException &e) {
                    // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                    std::string str = "abort\n";
                    memcpy(data_send, str.c_str(), str.length());
                    data_send[str.length()] = '\0';
                    offset = str.length();

                    // 回滚事务
                    txn_manager->abort(context.get(), log_manager.get());
                    std::cout << e.GetInfo() << std::endl;

                    std::fstream outfile;
                    outfile.open("output.txt", std::ios::out | std::ios::app);
                    outfile << str;
                    outfile.close();
                } catch (RMDBError &e) {
                    // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
                    // 遇到异常，打印异常信息
                    std::cerr << "RMDBError " << e.what() << std::endl;
                    memcpy(data_send, e.what(), e.get_msg_len());
                    data_send[e.get_msg_len()] = '\n';
                    data_send[e.get_msg_len() + 1] = '\0';
                    offset = e.get_msg_len() + 1;

                    // 将报错信息写入output.txt
                    std::fstream outfile;
                    outfile.open("output.txt", std::ios::out | std::ios::app);
                    outfile << "failure\n";
                    outfile.close();
                }
            }
        } else {
            // 语法分析失败，返回错误信息
            std::string str = "syntax error\n";
            memcpy(data_send, str.c_str(), str.length());
            data_send[str.length()] = '\0';
            offset = str.length();

            // 将报错信息写入output.txt
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << "failure\n";
            outfile.close();
        }
        if (finish_analyze == false) {
            yy_delete_buffer(buf);
            pthread_mutex_unlock(buffer_mutex);
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if (write(fd, data_send, offset + 1) == -1) {
            break;
        }
        // 如果是单挑语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
        if (context->txn_ != nullptr && context->txn_->get_txn_mode() == false) {
            txn_manager->commit(context->txn_, context->log_mgr_);
        }
    }

    // Clear
    std::cout << "Terminating current client_connection..." << std::endl;
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
    buffer_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    sockfd_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(buffer_mutex, nullptr);
    pthread_mutex_init(sockfd_mutex, nullptr);

    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in{};

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
        std::cout << "Bind error!" << std::endl;
        exit(1);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1) {
        std::cout << "Listen error!" << std::endl;
        exit(1);
    }

    while (!should_exit) {
        std::cout << "Waiting for new connection..." << std::endl;
        pthread_t thread_id;

        struct sockaddr_in s_addr_client{};
        int client_length = sizeof(s_addr_client);
        if (setjmp(jmpbuf)) {
            std::cout << "Break from Server Listen Loop\n";
            break;
        }

        // Block here. Until server accepts a new connection.
        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
        if (sockfd == -1) {
            std::cout << "Accept error!" << std::endl;
            continue;  // ignore current socket ,continue while loop.
        }

        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        if (pthread_create(&thread_id, nullptr, &client_handler, (void *)(&sockfd)) != 0) {
            std::cout << "Create thread fail!" << std::endl;
            break;  // break while loop
        }
    }

    // Clear
    std::cout << " Try to close all client-connection.\n";
    int ret = shutdown(sockfd_server, SHUT_WR);
    // shut down the all or part of a full-duplex connection.
    if (ret == -1) {
        printf("%s\n", strerror(errno));
    }
    //    assert(ret != -1);
    sm_manager->close_db();
    std::cout << " DB has been closed.\n";
    std::cout << "Server shuts down." << std::endl;
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
    try {
        std::cout << "\n"
                     "  _____  __  __ _____  ____  \n"
                     " |  __ \\|  \\/  |  __ \\|  _ \\ \n"
                     " | |__) | \\  / | |  | | |_) |\n"
                     " |  _  /| |\\/| | |  | |  _ < \n"
                     " | | \\ \\| |  | | |__| | |_) |\n"
                     " |_|  \\_\\_|  |_|_____/|____/ \n"
                     "\n"
                     "Welcome to RMDB!\n"
                     "Type 'help;' for help.\n"
                     "\n";
        // Database name is passed by args
        std::string db_name = argv[1];
        if (!sm_manager->is_dir(db_name)) {
            // Database not found, create a new one
            sm_manager->create_db(db_name);
        }
        // Open database
        sm_manager->open_db(db_name);

        // recovery database
        recovery->analyze();
        recovery->redo();
        recovery->undo();

        // 开启服务端，开始接受客户端连接
        start_server();
    } catch (RMDBError &e) {
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    return 0;
}
