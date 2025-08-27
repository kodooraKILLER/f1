# f1


# Kafka cluster setup (Local)

```bash
wsl -u root
sudo apt-get update
sudo apt install docker
docker pull apache/kafka:4.0.0
docker run -p 9092:9092 apache/kafka:4.0.0
```