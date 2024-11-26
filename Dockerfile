FROM flink:1.16.2
ARG FLINK_VERSION=1.16.2
ARG PYTHON_VERSION=3.9.18

# install python3 and pip3
RUN apt-get update -y && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev lzma liblzma-dev && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xvf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --without-tests --enable-shared && \
    make -j4 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-${PYTHON_VERSION}.tgz && rm -rf Python-${PYTHON_VERSION} && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    ln -s /usr/local/bin/pip3 /usr/local/bin/pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* 

RUN python3 -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple && \
    python3 -m pip install apache-flink==${FLINK_VERSION} -i https://pypi.tuna.tsinghua.edu.cn/simple  && \
    python3 -m pip install matplotlib && \
    python3 -m pip install kafka-python && \
    python3 -m pip install scikit-learn && \
    python3 -m pip install bloom-filter && \
    python3 -m pip install bloom_filter && \
    python3 -m pip install numpy && \
    python3 -m pip install paho-mqtt && \
    python3 -m pip install sklearn-json
    
# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/${FLINK_VERSION}/flink-sql-connector-elasticsearch7-${FLINK_VERSION}.jar;

RUN echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml;

RUN echo "taskmanager.numberOfTaskSlots: 4" >> /opt/flink/conf/flink-conf.yaml;
RUN echo "parallelism.default: 4" >> /opt/flink/conf/flink-conf.yaml;

WORKDIR /opt/flink
