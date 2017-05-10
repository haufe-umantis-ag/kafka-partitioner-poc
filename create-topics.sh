#cd $APACHE_KAFKA_HOME
DIR=`pwd`
APACHE_KAFKA_HOME=/Users/espinosa-rud/workspace/kafka/kafka_2.12-0.10.2.0
cd $APACHE_KAFKA_HOME
 ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 6 --topic partitioner-topic