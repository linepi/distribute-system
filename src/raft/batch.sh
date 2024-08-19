#!/bin/bash

# 最大运行时间（秒）
MAX_DURATION=$((30 * 60)) 

# 开始时间
START_TIME=$(date +%s)

# 运行次数计数器
iteration=0
loop=1

# 运行命令的函数
run_command() {
    for ((i=1; i<=32; i++)); do
      # go test -run TestReElection3A >> "$LOG_FILE" 2>&1
      # go test -run TestManyElections3A >> "$LOG_FILE" 2>&1
      /tmp/raft -test.run 3A >> /dev/null 2>&1 &
    done

    wait

    # 检查是否超过最大运行时间
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))

    if [ "$ELAPSED_TIME" -ge "$MAX_DURATION" ]; then
        echo "Maximum duration exceeded. Exiting."
        loop=0
    fi
}

# 循环运行命令，直到超过最大运行时间
go test -c -race -o /tmp/raft
while [ $loop -eq 1 ]; do
    run_command
done

rg FAIL logs

