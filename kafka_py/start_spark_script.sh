spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 ss_receiver_proc_aggregation_test.py
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
#   --executor-memory 8G \
#   --total-executor-cores 5 \
#   --driver-memory 8g \
#   ss_receiver_proc_aggregation_update.py