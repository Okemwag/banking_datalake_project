FROM apache/hive:standalone-metastore-4.2.0

USER root
COPY infra/docker/hive/core-site.xml.template /opt/hive/conf/core-site.xml.template
ADD https://jdbc.postgresql.org/download/postgresql-42.7.10.jar /opt/hive/lib/postgresql-42.7.10.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar /opt/hive/lib/hadoop-aws-3.4.1.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.720/aws-java-sdk-bundle-1.12.720.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.720.jar
ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar /opt/hive/lib/bundle-2.24.6.jar
