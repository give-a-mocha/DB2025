import argparse
import os
import signal
import subprocess
import threading
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import psutil

perf_process = None
server_process = None

def run_command_simple(command, timeout=1000, realtime=True):
    """简单的命令执行函数，支持实时输出"""
    print(f"正在运行命令: {command}")

    if not realtime:
        # 原有的非实时模式
        if command.strip().startswith('sudo'):
            modified_command = f"echo '{sudo_password}' | sudo -S {command[5:]}"
            result = subprocess.run(modified_command, shell=True, capture_output=True, 
                                      text=True, timeout=timeout)
            returncode = result.returncode
            stdout = result.stdout
            stderr = result.stderr
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
        global perf_process, server_process
        if command.strip().startswith('sudo'):
            modified_command = f"echo '{sudo_password}' | sudo -S {command[5:]}"
            process = subprocess.Popen(modified_command, shell=True,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     text=True, bufsize=1)
            if "perf" in command:
                perf_process = process
            elif "rmdb" in command:
                server_process = process
            time.sleep(0.5)
        else:
            process = subprocess.Popen(command, shell=True,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     text=True, bufsize=1)
            if "perf" in command:
                perf_process = process
            elif "rmdb" in command:
                server_process = process
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
    
def get_flame_graph(timestamp, mode='normal'):
    """生成火焰图，使用时间戳作为文件名"""
    base_path = current_dir
    
    # 使用时间戳和模式命名文件
    data_file = os.path.join(base_path, f"perf-{timestamp}.data")
    perf_file = os.path.join(base_path, f"perf-{timestamp}.perf")
    folded_file = os.path.join(base_path, f"perf-{timestamp}.folded")
    svg_file = os.path.join(base_path, f"flamegraph-{mode}-{timestamp}.svg")

    if mode == 'normal':
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && sudo perf script -i {data_file} --no-inline > {perf_file}")
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/stackcollapse-perf.pl {perf_file} > {folded_file}")
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/flamegraph.pl {folded_file} > {svg_file}")
    elif mode == 'diff':
        diff_folded_file = os.path.join(base_path, f"diff-{timestamp}.folded")
        if chaff_path is None:
            print("错误：差分模式需要一个基础 .folded 文件路径 (-c/--ch)。")
            return None
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && sudo perf script -i {data_file} --no-inline > {perf_file}")
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/stackcollapse-perf.pl {perf_file} > {folded_file}")
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/difffolded.pl {chaff_path} {folded_file} > {diff_folded_file}")
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/flamegraph.pl {diff_folded_file} > {svg_file}")
    elif mode == 'on-cpu':
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && sudo perf script -i {data_file} | {FlameGraph_path}/stackcollapse-perf.pl > {folded_file}")
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/flamegraph.pl --color=java {folded_file} > {svg_file}")
    elif mode == 'off-cpu':
        # 使用最通用的 stackcollapse-perf.pl 进行分步处理，以获得最佳兼容性
        stacks_file = os.path.join(base_path, f"out-{timestamp}.stacks")
        folded_file = os.path.join(base_path, f"perf-{timestamp}.folded")

        # 1. perf script with specific format
        script_command = f"export PATH=$PATH:{FlameGraph_path} && sudo perf script -i {data_file} -F comm,pid,tid,cpu,time,period,event,ip,sym,dso,trace > {stacks_file}"
        result = run_command_simple(script_command, realtime=False)
        if result['returncode'] != 0:
            print(f"错误：perf script 命令失败。\n{result['stderr']}")
            return None

        # 2. stackcollapse-perf.pl
        collapse_command = f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/stackcollapse-perf.pl {stacks_file} > {folded_file}"
        result = run_command_simple(collapse_command, realtime=False)
        if result['returncode'] != 0:
            print(f"错误：stackcollapse-perf.pl 命令失败。\n{result['stderr']}")
            return None
            
        # 3. flamegraph.pl
        flame_command = f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/flamegraph.pl --countname=us --color=io {folded_file} > {svg_file}"
        result = run_command_simple(flame_command, realtime=False)
        if result['returncode'] != 0:
            print(f"错误：flamegraph.pl 命令失败。\n{result['stderr']}")
            return None
    elif mode == 'memory':
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && sudo perf script -i {data_file} | {FlameGraph_path}/stackcollapse-perf.pl > {folded_file}")
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/flamegraph.pl --color=mem {folded_file} > {svg_file}")
    elif mode == 'lock':
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && sudo perf script -i {data_file} | {FlameGraph_path}/stackcollapse-perf.pl > {folded_file}")
        run_command_simple(f"export PATH=$PATH:{FlameGraph_path} && {FlameGraph_path}/flamegraph.pl {folded_file} > {svg_file}")

    # 清理中间文件
    run_command_simple(f"rm -f {data_file}")
    if os.path.exists(perf_file):
        run_command_simple(f"rm -f {perf_file}")
    if os.path.exists(folded_file):
        run_command_simple(f"rm -f {folded_file}")
    if mode == 'diff':
        diff_folded_file = os.path.join(base_path, f"diff-{timestamp}.folded")
        if os.path.exists(diff_folded_file):
            run_command_simple(f"rm -f {diff_folded_file}")
    if mode == 'off-cpu':
        # 清理 off-cpu 模式的中间文件
        stacks_file = os.path.join(base_path, f"out-{timestamp}.stacks")
        folded_file = os.path.join(base_path, f"perf-{timestamp}.folded")
        if os.path.exists(stacks_file):
            run_command_simple(f"rm -f {stacks_file}")
        if os.path.exists(folded_file):
            run_command_simple(f"rm -f {folded_file}")
        
    return svg_file


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='数据库性能测试工具')
    parser.add_argument('-w', '--warehouse', type=int, required=True, 
                       help='数据量大小（必需）')
    parser.add_argument('-p', '--password', type=str, required=True,
                       help='sudo 密码（必需）')
    parser.add_argument('-t', '--tpcc_path', type=str, required=True,
                       help='TPCC 路径（必需）')
    parser.add_argument('-f', '--flamegraph_path', type=str, required=True,
                       help='FlameGraph 路径（必需）')

    parser.add_argument('-c', '--ch', type=str, help='差分路径（必需）')
    parser.add_argument('-m', '--flame_mode', type=str, default='normal',
                        choices=['normal', 'diff', 'on-cpu', 'off-cpu', 'memory', 'lock'],
                        help='火焰图生成模式: normal, diff, on-cpu, off-cpu')
    
    args = parser.parse_args()


    # data_size = int(input("请输入数据量: "))
    # sudo_password = input("请输入sudo密码: ")
    # test_path = input("请输入TPCC路径: ")
    # FlameGraph_path = input("请输入FlameGraph路径: ")

    data_size = args.warehouse
    sudo_password = args.password
    test_path = args.tpcc_path
    FlameGraph_path = args.flamegraph_path
    chaff_path = args.ch
    flame_mode = args.flame_mode



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
    run_command_simple(compile_db)
    run_command_simple(gen_data_command)
    
    futures = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 提交数据库启动任务
        print("启动数据库...")
        db_future = executor.submit(run_command_simple, start_db)
        
        # 等待数据库启动并获取PID
        time.sleep(1)  # 给数据库更多启动时间
        pid = get_pid()

        # 使用时间戳命名性能数据文件
        perf_data_file = os.path.join(current_dir, f"perf-{timestamp}.data")
        print(f"性能数据文件: {perf_data_file}")
        if flame_mode in ['normal', 'diff']:
            start_monitor = f"sudo perf record -F 5000 -p {pid} -g -o {perf_data_file} --call-graph=dwarf && echo '性能测试成功开启'"
        elif flame_mode == 'on-cpu':
            start_monitor = f"sudo perf record -F 99 -p {pid} -g -o {perf_data_file} && echo 'On-CPU性能测试成功开启'"
        elif flame_mode == 'off-cpu':
            start_monitor = f"sudo perf record -e 'sched:sched_stat_sleep' -e 'sched:sched_switch' -e 'sched:sched_process_exit' -p {pid} -g -o {perf_data_file} && echo 'Off-CPU性能测试成功开启'"
        elif flame_mode == 'memory':
            start_monitor = f"sudo perf record -e page-faults -p {pid} -g -o {perf_data_file} && echo '内存页错误性能测试成功开启'"
        elif flame_mode == 'lock':
            start_monitor = f"sudo perf record -e lock:contended -p {pid} -g -o {perf_data_file} && echo '锁竞争性能测试成功开启'"
        start_test = f"cd {test_path} && cd .. && python TPCC-Tester/runner.py --prepare --thread 8 --rw 150 --analyze --w {data_size}"
        
        print("启动性能监控...")
        monitor_feature = executor.submit(run_command_simple, start_monitor)
        print(f"性能监控pid: {perf_process.pid}")
        
        # 给perf命令一点时间启动
        time.sleep(1)
        
        print("启动测试程序...")
        test_future = executor.submit(run_command_simple, start_test)

        
        # 获取结果
        test_future.result()
        print("测试完成!")

        # run_command_simple(f"sudo kill {pid}")
        # psutil.Process(pid).kill()

        subprocess.Popen(f"sudo kill {pid}", shell=True)

        # server_process.send_signal(signal.SIGINT)
        # server_process.wait(timeout=5)
        # db_future.result()


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
        # subprocess.Popen(f"sudo kill {perf_process.pid}", shell=True)
        print("性能监控结束")

        # executor.shutdown(wait=False)

        

    
        # 生成火焰图
        print("开始生成火焰图...")
        flame_graph_file = get_flame_graph(timestamp, flame_mode)
        print(f"测试完成！火焰图文件: {flame_graph_file}")


        run_command_simple(rm_data_command)
        run_command_simple(rm_db)

        os._exit(0)
