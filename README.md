# Steps to run the docker file

```shell
docker compose up --build
```

# The URLs

## For spark logging

```url
http://localhost:8080/
```

## For MiniO

```url
http://127.0.0.1:9001
```

The most used command inside the docker containers.


```bash
docker exec -it spark-iceberg bash
```


```bash
ls /home/spark/scripts
```

```bash
cd /home/spark/scripts
```

```bash
spark-submit iceberg_orc.py
```

```cmd
python3 iceberg_orc.py
```

```
apt-get update && apt-get install -y iputils-ping
```

```
curl http://minio:9000/minio/health/live
```

```
ping minio
```
