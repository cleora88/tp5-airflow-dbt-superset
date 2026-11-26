# Image Spark du TP3 : Spark 3.5.9 officiel, plus les trois JAR du lakehouse
FROM apache/spark:3.5.9-python3

ARG MAVEN=https://repo.maven.apache.org/maven2

# Iceberg pour Spark 3.5 (Scala 2.12)
ADD --chown=spark:spark ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar /opt/spark/jars/
# Connecteur S3A de Hadoop 3.3.4 (la version embarquée par Spark 3.5) et le SDK AWS qu'il attend
ADD --chown=spark:spark ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chown=spark:spark ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

WORKDIR /opt/tp3
