# 生成 cluster.id
CLUSTER_ID=$(kafka-storage.sh random-uuid)
echo "Generated cluster id: $CLUSTER_ID"

# 初始化日志目录（会生成 meta.properties）
kafka-storage.sh format \
  --config /Users/zhiqin.zhang/Documents/SourceCode/kafka/config/server.properties \
  --cluster-id "$CLUSTER_ID"