#!/bin/sh

# Setup variables
# CHANGE FOR YOUR ENV: absolute path of the indexer installation dir
INDEXER_HOME=/home/jfd/soft/kafka-elasticsearch-consumer

# CHANGE FOR YOUR ENV: JDK 8 installation dir - you can skip it if your JAVA_HOME env variable is set
JAVA_HOME=/home/jfd/soft/jdk1.8.0_112

# CHANGE FOR YOUR ENV: absolute path of the logback config file
LOGBACK_CONFIG_FILE=/home/jfd/soft/kafka-elasticsearch-consumer/build/resources/main/config/logback.xml

# CHANGE FOR YOUR ENV: absolute path of the indexer properties file
INDEXER_PROPERTIES_FILE=/home/jfd/soft/kafka-elasticsearch-consumer/build/resources/main/config/kafka-es-indexer.properties

# DO NOT CHANGE ANYTHING BELOW THIS POINT (unless you know what you are doing :) )!
echo "Starting Kafka ES Indexer app ..."
echo "INDEXER_HOME=$INDEXER_HOME"
echo "JAVA_HOME=$JAVA_HOME"
echo "LOGBACK_CONFIG_FILE=$LOGBACK_CONFIG_FILE"
echo "INDEXER_PROPERTIES_FILE=$INDEXER_PROPERTIES_FILE"

# add all dependent jars to the classpath
for file in $INDEXER_HOME/build/install/kafka-elasticsearch-consumer/lib/*.jar;
do
  CLASS_PATH=$CLASS_PATH:$file
done
echo "CLASS_PATH=$CLASS_PATH"

$JAVA_HOME/bin/java -Xmx1g -cp $CLASS_PATH -Dindexer.properties=$INDEXER_PROPERTIES_FILE -Dlogback.configurationFile=$LOGBACK_CONFIG_FILE org.elasticsearch.kafka.indexer.KafkaESIndexerProcess




