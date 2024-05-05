# Steps to run the docker file

First step is to start all the environment required for spark. Just enter this command after installing docker.

```shell
docker compose up --build
```

Verify these urls once the above server is running.

# The URLs

MiniO is S3 simulator in local so this program can be easly migrated to S3
## For MiniO

```url
http://127.0.0.1:9001
```

This one to view all the spark jobs that has ran and running
## For spark logging

```url
http://localhost:8080/
```

## Once the URL verified 

1. Open new terminal and enter this command to go into spark container to run the spark

```bash
docker exec -it spark-iceberg bash
```

2. Moving into specific directory where spark exists

```bash
cd /home/spark/scripts
```
3. For running the spark file. After running this file you can see the output in terminal or in the MiniO Url

```bash
spark-submit iceberg_orc.py
```





# Other commonly used commands

```bash
ls /home/spark/scripts
```

```bash
python3 iceberg_orc.py
```

```bash
apt-get update && apt-get install -y iputils-ping
```

```bash
curl http://minio:9000/minio/health/live
```

```bash
ping minio
```
