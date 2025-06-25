#!/bin/bash

# 创建 topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-batch --partitions 1 --replication-factor 1

# 启动 producer（需用 Java 编译）
java -cp ./your-jar-path BatchProducer

# 找到日志文件
LOG_FILE=$(find /tmp/kraft-combined-logs/ -name "*.log" | grep "test-batch")

# 解析 RecordBatch
bin/kafka-dump-log.sh --files $LOG_FILE --print-data-log --deep-iteration