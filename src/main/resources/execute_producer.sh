export HADOOP_CONF_DIR=/home/hdoopusr/hadoop-2.7.7/etc/hadoop

#spark config
DRIVERHEAP="--driver-memory 1g"
WORKERHEAP="--executor-memory 1g"
NBWROKER="--num-executors 1"
NBCORE="--executor-cores 1"
CONF_FILE="/home/hdoopusr/application.conf"
SPARK_CONF="--conf spark.executor.extraJavaOptions=\"-Dconfig.file=./application.conf\" \
--conf spark.driver.extraJavaOptions=\"-Dconfig.file=./application.conf\" \
--files ${CONF_FILE}"


MAINCLASS=Producer
JAR=/home/hdoopusr/kafka-twitter-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit ${DRIVERHEAP} ${WORKERHEAP} ${NBWORKER} ${NBCORE} --master yarn --deploy-mode cluster ${SPARK_CONF} --class ${MAINCLASS} ${JAR}

