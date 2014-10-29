$SPARK_HOME/bin/spark-submit \
    --class KafkaTest \
    --master yarn-cluster \
    /home/bdp_jdmp_da/yangdekun/code/parameter-server/kafka_test/target/nosql-ps-0.1-SNAPSHOT-jar-with-dependencies.jar \
    172.22.178.127:2181,172.22.178.128:2181,172.22.178.179:2181/kafka test-consumer stream-mssql-parser-3-dbo-orders 10 
