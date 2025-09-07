# f1


## Kafka cluster setup (Local)

```bash
wsl -u root
# sudo apt-get update
# sudo apt install docker
# docker pull apache/kafka:4.0.0
docker run -p 9092:9092 apache/kafka:4.0.0
```

## Flink cluster setup (Local)

```bash
wsl -u root
# java -version
# download flink tgz
# tar -xzf flink-*.tgz
cd flink-2.1.0
flink-2.1.0/bin/start-cluster.sh
cd f1/f1/flink
./../../../flink-2.1.0/bin/flink run -pyclientexec /usr/bin/python3.9 -pyexec /usr/bin/python3.9 -py test.py

```
