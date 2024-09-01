#!/bin/bash

# 最大运行时间（秒）
MAX_DURATION=$((60 * 30))

# 开始时间
START_TIME=$(date +%s)

finish=0
parallel=2
batch_id=batch$(date '+%Y-%m-%d-%H-%M-%S')
batch_log_dir=./logs/${batch_id}
program=/tmp/raft-"$batch_id"

loopi=1

# 运行命令的函数
run_command() {
    echo "-------------- loop ${loopi} -------------------"
    for ((i=1; i<=parallel; i++)); do
      # go test -run TestReElection3A >> "$LOG_FILE" 2>&1
      # go test -run TestManyElections3A >> "$LOG_FILE" 2>&1
      # echo "RAFT_LOG_DIR=${batch_log_dir} ${program} -test.run 3B >> /dev/null 2>&1 &"
      RAFT_LOG_DIR=$batch_log_dir ${program} -test.run 3B &
    done

    wait

    # 检查是否超过最大运行时间
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))

    if [ "$ELAPSED_TIME" -ge "$MAX_DURATION" ]; then
        echo "Maximum duration exceeded. Exiting."
        finish=1
    fi
    ((loopi+=1))
}

# 循环运行命令，直到超过最大运行时间
go test -c -race -o "${program}"
while [ $finish -eq 0 ]; do
    run_command
    out=$(rg "FAIL|debug.Stack" "${batch_log_dir}")
    if [[ "$out" -ne "" ]]; then
      echo "FAIL, output:"
      echo out
      break
    fi
done


