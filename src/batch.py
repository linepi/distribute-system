import argparse
import os
import subprocess
import time
import signal
import threading

procs = []
running = True
data_lock = threading.Lock()

def signal_handler(sig, frame):
    print("...killing all processes")
    for proc in procs:
        proc.terminate()  # 发送终止信号给子进程
    global running
    running = False

class Cfg:
    def __init__(self, args):
        self.batch_id = f"batch{time.strftime('%Y-%m-%d-%H-%M-%S')}"
        self.batch_log_dir = f"./logs/{self.batch_id}"
        os.makedirs(self.batch_log_dir, exist_ok=True)
        self.program = f"/tmp/raft-{self.batch_id}"
        self.parallel = args.parallel
        self.max_duration = args.run_time
        self.test = args.test
        self.command_timeout = args.time_out
        self.stop_fail_rate = args.stop_fail_rate

class Data:
    def __init__(self):
        self.success = 0
        self.failure = 0
        self.timeout = 0
        self.all = 0
        self.running = 0
        self.timeout_logfiles = []
        self.failure_logfiles = []
        self.success_logfiles = []
        self.starttime = time.time()
        self.total_run_time = 0.0  # 新增总运行时间

    def show(self):
        used = '%.1f' % (time.time() - self.starttime)
        average_run_time = '%.2f' % (self.total_run_time / self.all) if self.all > 0 else '0.00'
        print(f"\r{used}s running: {self.running}, "
              f"suc: {self.success}, timeout: {self.timeout}, failure: {self.failure}, "
              f"avg_run_time: {average_run_time}s  \b\b", end='')

class OutputAnalyzer:
    def __init__(self, output: str):
        self.output = output

    def logfile(self):
        return self.output.splitlines()[0][10:]

def handle_proc(proc, cfg, data):
    global running
    start_time = time.time()  # 记录开始时间
    stdout, _ = proc.communicate()
    end_time = time.time()    # 记录结束时间
    run_time = end_time - start_time  # 计算运行时间

    if not running:
        return
    output_analyzer = OutputAnalyzer(stdout)
    with data_lock:
        data.running -= 1
        data.all += 1
        data.total_run_time += run_time  # 累加运行时间

        exit_status = proc.returncode
        if exit_status == 124:
            data.timeout += 1
            data.timeout_logfiles.append(output_analyzer.logfile())
        elif exit_status == 0:
            data.success += 1
            data.success_logfiles.append(output_analyzer.logfile())
        else:
            data.failure += 1
            data.failure_logfiles.append(output_analyzer.logfile())
            print(f'\n{stdout}')
            t1 = cfg.stop_fail_rate
            t2 = (data.all - data.success) / data.all
            if t2 > t1:
                print(f"\nSTOP FAIL RATE REACHED {t2}, bigger than {t1}")
                running = False
        data.show()

def run_command(cfg: Cfg, data: Data):
    global procs
    global running
    procs = []
    threads = []

    for i in range(1, cfg.parallel + 1):
        command = ["timeout", str(cfg.command_timeout), cfg.program, "-test.run", cfg.test]
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env={'RAFT_LOG_DIR': cfg.batch_log_dir}, text=True)
        procs.append(proc)
        with data_lock:
            data.running += 1
            data.show()

    for proc in procs:
        t = threading.Thread(target=handle_proc, args=(proc, cfg, data))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return running

def parse():
    parser = argparse.ArgumentParser(description="Run test with parameters.")
    parser.add_argument('--test', default="", help="Test to run.")
    parser.add_argument('--run_time', type=int, required=True, help="Max run time (in seconds).")
    parser.add_argument('--parallel', type=int, required=True, help="Number of parallel processes.")
    parser.add_argument('--stop_fail_rate', type=float, default=1, help="Stop fail rate.")
    parser.add_argument('--time_out', type=int, default=300, help="Time out duration for each test.")
    return parser.parse_args()

def main():
    signal.signal(signal.SIGINT, signal_handler)
    args = parse()
    cfg = Cfg(args)
    data = Data()
    start_time = time.time()

    subprocess.run(["go", "test", "-c", "-race", "-o", cfg.program], check=True)
    print(f'batch_log_dir: {cfg.batch_log_dir}')

    global running
    while running:
        running = run_command(cfg, data)
        elapsed_time = time.time() - start_time
        if elapsed_time >= cfg.max_duration:
            break

    time.sleep(5)
    print("\r------------------------------------- end ----------------------------------------")
    print(f"parallel: {args.parallel}, test: {args.test}")
    print(f"success: {data.success}, fail: {data.failure}, timeout: {data.timeout}")

    print(f"Average Run Time: {'%.2f' % (data.total_run_time / data.all) if data.all > 0 else '0.00'}s")

    for logfile in data.success_logfiles:
        try:
            os.remove(logfile)
        except OSError:
            pass

    if data.timeout > 0:
        print("@timeout logfiles")
        for logfile in data.timeout_logfiles:
            print(logfile)

    if data.failure > 0:
        print("@fail logfiles")
        for logfile in data.failure_logfiles:
            print(logfile)

if __name__ == "__main__":
    main()
