FROM python
COPY ./requirements.txt ./
RUN pip install -r ./requirements.txt

USER root
  # Install OpenJDK-11
RUN apt update &&  apt-get install -y openjdk-17-jdk &&  apt-get install -y ant &&  apt-get clean;
# Set JAVA_HOME
ENV JAVA_HOME /usr/
RUN export JAVA_HOME

#Download Hudi Jars inside jars folder

## Spark version 3.5.0
## Hudi version 1.0.0
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar -P ./jars/

RUN wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.0.0/hudi-spark3.5-bundle_2.12-1.0.0.jar -P ./jars/
