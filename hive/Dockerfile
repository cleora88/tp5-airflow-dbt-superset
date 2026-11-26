# Image du Hive Metastore du TP3 : metastore officiel 4.2.1, plus l'accès à MySQL et à MinIO

# Étape 1 : récupérer le module S3 du SDK AWS v2 avec Ivy, l'outil de dépendances déjà présent dans l'image Spark
FROM apache/spark:3.5.9-python3 AS sdk
USER root
COPY ivysettings.xml /tmp/ivysettings.xml
RUN java -jar /opt/spark/jars/ivy-2.5.3.jar -settings /tmp/ivysettings.xml \
      -dependency software.amazon.awssdk s3-transfer-manager 2.24.6 -confs default \
      -retrieve "/sdk/[artifact]-[revision].[ext]"

# Étape 2 : l'image du metastore
FROM apache/hive:standalone-metastore-4.2.1
COPY --from=sdk /sdk/ /opt/hive/lib/
# Le connecteur S3A est livré avec Hadoop dans l'image, mais hors du classpath du metastore
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.4.1.jar /opt/hive/lib/
# Le pilote MySQL, le même qu'au TP1 et au TP2
COPY mysql-connector-j-8.4.0.jar /opt/hive/lib/
