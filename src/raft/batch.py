import argparse
import os
import subprocess
import time
import signal

procs = []
running = True

def signal_handler(sig, frame):
    print("...killing all process")
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

    def show(self):
        used = '%.1f' % (time.time() - self.starttime)
        print(f"\r{used}s running: {self.running}, finished: {self.all}, suc: {self.success}", end='')

""" 
Output Like This:
WSL √ ~/dev/6.5840/src/raft $ go test -race -run 3A
Log path: ./logs/log_2024-09-09-13-08-17_568293.txt
Test (3A): initial election ...
  ... Passed --   3.0  3   44   16602    0
Test (3A): election after network failure ...
  ... Passed --   6.0  3  141   30230    0
Test (3A): multiple elections ...
  ... Passed --   6.1  7  604  138182    0
PASS
ok      6.5840/raft     16.135s
"""
class OutputAnalyzer:
    def __init__(self, output: str):
        self.output = output

    def logfile(self):
        return self.output.splitlines()[0][10:]

def run_command(cfg: Cfg, data: Data):
    global procs
    global running
    procs = []

    for i in range(1, cfg.parallel + 1):
        command = ["timeout", str(cfg.command_timeout), cfg.program, "-test.run", cfg.test]
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env={'RAFT_LOG_DIR': cfg.batch_log_dir}, text=True)
        procs.append(proc)
        data.running += 1
        data.show()

    for i, proc in enumerate(procs):
        stdout = proc.communicate()[0]
        if not running:
            return False
        output_analyzer = OutputAnalyzer(stdout)
        data.running -= 1
        data.all += 1

        exit_status = proc.returncode
        if exit_status == 124:
            data.timeout += 1
            data.timeout_logfiles.append(output_analyzer.logfile())
        elif exit_status != 0:
            data.failure += 1
            data.failure_logfiles.append(output_analyzer.logfile())
            t1 = cfg.stop_fail_rate
            t2 = (data.all - data.success) / data.all
            if t2 > t1:
                print(f"STOP FAIL RATE REACHED {t2}, bigger than {t1}")
                return False
        else:
            data.success += 1
            data.success_logfiles.append(output_analyzer.logfile())
        data.show()

    return True

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
    print("\r------------------------- end ----------------------------")
    print(f"parallel: {args.parallel}, test: {args.test}")
    print(f"success: {data.success}, fail: {data.failure}, timeout: {data.timeout}")

    for logfile in data.success_logfiles:
        try:
            os.remove(logfile)
        except OSError as e:
            print(e)

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
