#!/bin/bash

RAFT_TEST_PARAMETER="$RAFT_TEST"
if [[ "$RAFT_TEST" == "" ]]; then
  RAFT_TEST="all"
fi

if [[ "$RAFT_RUN_TIME" == "" ]]; then
  RAFT_RUN_TIME=30
fi
if [[ "$RAFT_PARALLEL" == "" ]]; then
  RAFT_PARALLEL=4
fi
if [[ "$RAFT_STOP_FAIL_RATE" == "" ]]; then
  RAFT_STOP_FAIL_RATE=100
fi
if [[ "$RAFT_TIME_OUT" == "" ]]; then
  RAFT_TIME_OUT=300
fi

# 最大运行时间（秒）
MAX_DURATION=$((RAFT_RUN_TIME))

# 开始时间
START_TIME=$(date +%s)

finish=0
parallel=$RAFT_PARALLEL
batch_id=batch$(date '+%Y-%m-%d-%H-%M-%S')
batch_log_dir=./logs/${batch_id}
echo "batch_log_dir is $batch_log_dir"
mkdir -p "$batch_log_dir"
program=/tmp/raft-"$batch_id"

loopi=0

# 数据
res_success=0
res_fail=0
res_timeout=0
res_all=0
timeout_logfiles=""
fail_logfiles=""
success_logfiles=""

printf "running: 0, finished: 0, suc: 0               "
# 运行命令的函数
run_command() {
    # echo "-------------- loop ${loopi} -------------------"
    running=0
    for ((i=1; i<=parallel; i++)); do
        timeout "$RAFT_TIME_OUT" env RAFT_LOG_DIR="$batch_log_dir" "$program" -test.run "$RAFT_TEST_PARAMETER" > \
            "$batch_log_dir/stdout_$i.txt" 2>&1 &
        pid=$!
        # 将 PID 和对应的任务标识存储到一个数组中
        pid_array[i]=$pid
        ((running+=1))
        printf "\rrunning: %d, finished: %d, suc: %d            " "$running" "$res_all" "$res_success"
    done

    # 等待所有后台任务完成
    for ((i=1; i<=parallel; i++)); do
        wait "${pid_array[$i]}"
        ((running-=1))
        ((res_all+=1))

        stdout_file="$batch_log_dir/stdout_$i.txt"
        log_file_line=$(head -n 1 < "$stdout_file")
        log_file=${log_file_line:10}

        exit_status=$?
        if [[ $exit_status -eq 124 ]]; then
            ((res_timeout+=1))
            timeout_logfiles="$timeout_logfiles\n$log_file"
            continue
        fi

        if [[ $exit_status -ne 0 ]]; then
          ((res_fail+=1))
          fail_logfiles="$fail_logfiles\n$log_file"
          # (res_all-res_success)/res_all > $RAFT_STOP_FAIL_RATE/100
          t1=$((res_all*RAFT_STOP_FAIL_RATE))
          t2=$((100*(res_all-res_success)))
          if [[ t2 -gt t1 ]]; then
            echo "STOP FAIL RATE REACH"
            finish=1
            break
          fi
        else
          ((res_success+=1))
          success_logfiles="$success_logfiles $log_file"
        fi
        printf "\rrunning: %d, finished: %d, suc: %d             " "$running" "$res_all" "$res_success"
    done

    # 检查是否超过最大运行时间
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))

    if [ "$ELAPSED_TIME" -ge "$MAX_DURATION" ]; then
        finish=1
    fi
    ((loopi+=1))
}

# 循环运行命令，直到超过最大运行时间
go test -c -race -o "${program}"
while [ $finish -eq 0 ]; do
    run_command
done

rm "$batch_log_dir"/stdout_*.txt
sleep 1
success_logfiles=("$success_logfiles")
# shellcheck disable=SC2128
for file in $success_logfiles; do
  rm -f "$file"
done

printf "\r"
echo "--------------- end ---------------------"
echo "loop: $loopi, parallel: $RAFT_PARALLEL, test: $RAFT_TEST"
echo "success: $res_success, fail: $res_fail, timeout: $res_timeout"

if [ $res_timeout -ne 0 ]; then
  echo "@timeout logfiles"
  echo -e "$timeout_logfiles"
fi

if [ $res_fail -ne 0 ]; then
  echo "@fail logfiles"
  echo -e "$fail_logfiles"
fi
