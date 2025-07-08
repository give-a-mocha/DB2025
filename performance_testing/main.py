import argparse
import os
import signal
import subprocess
import threading
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

perf_process = None

def run_command_simple(command, timeout=1000, realtime=True):
    """简单的命令执行函数，支持实时输出"""
    print(f"正在运行命令: {command}")

    
    if not realtime:
        # 原有的非实时模式
        if command.strip().startswith('sudo') and sudo_password:
            modified_command = command.replace('sudo ', 'sudo -S ', 1)
            process = subprocess.Popen(modified_command, shell=True, 
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                                     stdin=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate(input=f"{sudo_password}\n", timeout=timeout)
            returncode = process.returncode
        else:
            try:
                result = subprocess.run(command, shell=True, capture_output=True, 
                                      text=True, timeout=timeout)
                returncode = result.returncode
                stdout = result.stdout
                stderr = result.stderr
            except subprocess.TimeoutExpired:
                print(f"命令超时: {command}")
                return {
                    'command': command,
                    'returncode': -1,
                    'stdout': '',
                    'stderr': '命令执行超时'
                }
    else:
        # 实时输出模式
        global perf_process
        if command.strip().startswith('sudo') and sudo_password:
            modified_command = command.replace('sudo ', 'sudo -S ', 1)
            process = subprocess.Popen(modified_command, shell=True,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     stdin=subprocess.PIPE, text=True, bufsize=1)
            if "perf" in command:
                perf_process = process

            process.stdin.write(f"{sudo_password}\n")
            process.stdin.flush()
            process.stdin.close()
        else:
            process = subprocess.Popen(command, shell=True,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     text=True, bufsize=1)
            if "perf" in command:
                perf_process = process
        stdout_lines = []
        try:
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    print(output.strip())
                    stdout_lines.append(output)
            
            returncode = process.wait(timeout=timeout)
            stdout = ''.join(stdout_lines)
            stderr = ''
            
        except subprocess.TimeoutExpired:
            print(f"命令超时: {command}")
            process.kill()
            process.wait()
            return {
                'command': command,
                'returncode': -1,
                'stdout': '',
                'stderr': '命令执行超时'
            }
    
    return {
        'command': command,
        'returncode': returncode,
        'stdout': stdout,
        'stderr': stderr
    }

def get_pid():
    # 等待数据库启动
    time.sleep(2)
    
    # 获取PID，跳过标题行，取第一条数据记录的PID
    get_pid = "lsof -i:8765"
    pid_result = run_command_simple(get_pid)
    if pid_result['returncode'] == 0 and pid_result['stdout'].strip():
        lines = pid_result['stdout'].strip().split('\n')
        if len(lines) > 1:  # 确保有数据行（除了标题行）
            # 第一行是标题，第二行是第一条数据记录
            pid = lines[1].split()[1]
            print(f"获取到的PID: {pid}")
        else:
            print("未找到进程")
            pid = None
    else:
        print("获取PID失败")
        pid = None

    if pid is None:
        os._exit(1)
    return pid
    
def get_flame_graph(timestamp):
    """生成火焰图，使用时间戳作为文件名"""
    base_path = current_dir
    
    # 使用时间戳命名文件
    data_file = os.path.join(base_path, f"perf-{timestamp}.data")
    perf_file = os.path.join(base_path, f"perf-{timestamp}.perf")
    folded_file = os.path.join(base_path, f"perf-{timestamp}.folded")
    svg_file = os.path.join(base_path, f"flamegraph-{timestamp}.svg")
    
    # 修改 perf script 命令，禁用符号解析或使用不同选项
    run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && sudo perf script -i {data_file} --no-inline > {perf_file}")
    
    # 或者使用 --no-demangle 选项
    # run_command_simple(f"sudo perf script -i {data_file} --no-demangle > {perf_file}")
    
    run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && stackcollapse-perf.pl {perf_file} > {folded_file}")
    run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && flamegraph.pl {folded_file} > {svg_file}")
    
    # 清理中间文件，保留最终的SVG文件
    run_command_simple(f"rm -rf {data_file}")
    run_command_simple(f"rm -rf {perf_file}")
    run_command_simple(f"rm -rf {folded_file}")
    
    return svg_file


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='数据库性能测试工具')
    parser.add_argument('-w', '--warehouse', type=int, required=True, 
                       help='数据量大小（必需）')
    parser.add_argument('-p', '--password', type=str, required=True,
                       help='sudo 密码（必需）')
    parser.add_argument('-t', '--tpcc-path', type=str, required=True,
                       help='TPCC 路径（必需）')
    parser.add_argument('-f', '--flamegraph-path', type=str, required=True,
                       help='FlameGraph 路径（必需）')
    
    args = parser.parse_args()


    # data_size = int(input("请输入数据量: "))
    # sudo_password = input("请输入sudo密码: ")
    # test_path = input("请输入TPCC路径: ")
    # FlameGraph_path = input("请输入FlameGraph路径: ")

    data_size = args.warehouse
    sudo_password = args.password
    test_path = args.tpcc_path
    FlameGraph_path = args.flamegraph_path


    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.dirname(current_dir)

    if db_path[-1] == '/': db_path = db_path[:-1]
    if test_path[-1] == '/': test_path = test_path[:-1]
    if FlameGraph_path[-1] == '/': FlameGraph_path = FlameGraph_path[:-1]
    
    # 生成时间戳用于文件命名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"时间戳: {timestamp}")


    rm_data_command = f"rm -rf {db_path}/src/test/performance_test/table_data/*"
    rm_db = f"rm -rf {db_path}/build/rmdb/"
    compile_db = f"cd {db_path}/build/ && cmake .. && make rmdb -j16"
    gen_data_command = f"python {test_path}/db/load.py -w {data_size} -o {db_path}/src/test/performance_test/table_data"
    start_db = f"cd {db_path}/build/ && ./bin/rmdb rmdb"
    

    run_command_simple(rm_data_command)
    run_command_simple(rm_db)
    run_command_simple(gen_data_command)
    
    futures = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 提交数据库启动任务
        print("启动数据库...")
        db_future = executor.submit(run_command_simple, start_db)
        
        # 等待数据库启动并获取PID
        time.sleep(3)  # 给数据库更多启动时间
        pid = get_pid()

        # 使用时间戳命名性能数据文件
        perf_data_file = os.path.join(current_dir, f"perf-{timestamp}.data")
        print(f"性能数据文件: {perf_data_file}")
        start_monitor = f"export PATH=$PATH:{FlameGraph_path} && sudo perf record -F 2000 -p {pid} -g -o {perf_data_file} --call-graph=dwarf"
        start_test = f"cd {test_path} && cd .. && python TPCC-Tester/runner.py --prepare --thread 8 --rw 150 --ro 150 --analyze --w {data_size}"
        
        print("启动性能监控...")
        monitor_feature = executor.submit(run_command_simple, start_monitor)
        print(f"性能监控pid: {perf_process.pid}")
        
        # 给perf命令一点时间启动
        time.sleep(2)
        
        print("启动测试程序...")
        test_future = executor.submit(run_command_simple, start_test)

        
        # 获取结果
        test_future.result()
        print("测试完成!")

        run_command_simple(f"sudo kill -9 {pid}")


        print("正在等待性能监控结束")
        if perf_process and perf_process.poll() is None:  # 检查进程是否还在运行
            try:
                perf_process.send_signal(signal.SIGINT)
                print("已发送 SIGINT 信号，等待进程结束...")
                perf_process.wait(timeout=10)  # 等待最多10秒
            except subprocess.TimeoutExpired:
                print("SIGINT 超时，强制 kill...")
                perf_process.kill()
                perf_process.wait()
            except ProcessLookupError:
                print("进程已结束")
                
        # 等待 monitor_feature 线程结束
        monitor_feature.result()
        print("性能监控结束")

        

    
    # 生成火焰图
    print("开始生成火焰图...")
    flame_graph_file = get_flame_graph(timestamp)
    print(f"测试完成！火焰图文件: {flame_graph_file}")


    run_command_simple(rm_data_command)
    run_command_simple(rm_db)
