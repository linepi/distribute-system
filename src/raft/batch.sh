#!/bin/bash

# 定义日志文件名
LOG_FILE="logs/batchlog.txt"

# 最大运行时间（秒）
MAX_DURATION=$((60 * 60)) 

# 开始时间
START_TIME=$(date +%s)

# 运行次数计数器
iteration=0

# 运行命令的函数
run_command() {
    iteration=$((iteration + 1))
    echo "Running command iteration $iteration" >> "$LOG_FILE"

    # 运行命令并将输出追加到日志文件
    # go test -run TestReElection3A >> "$LOG_FILE" 2>&1
    go test -run 3A >> "$LOG_FILE" 2>&1
    # go test -run TestManyElections3A >> "$LOG_FILE" 2>&1

    # 检查是否超过最大运行时间
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))

    if [ "$ELAPSED_TIME" -ge "$MAX_DURATION" ]; then
        echo "Maximum duration exceeded. Exiting."
        exit 0
    fi
}

# 循环运行命令，直到超过最大运行时间
while true; do
    run_command
done

