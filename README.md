# ProteinIdentificationGPU
Identification of proteins in DNA accelerated and executed on GPUs

Steps followed:
1. Started cassandra on localhost: Cassandra.bat
2. Installed python's cassandra-driver and python-kafka driver
3. Started zookeeper (bin/zookeeper-server-start.sh config/zookeeper.properties) #changed zookeper port to 2180 + config/server
4. Started kafka server (bin/kafka-server-start.sh config/server.properties)
5. Created a topic (bin/kafka-topics.sh --create --zookeeper localhost:2180 --replication-factor 1 --partitions 1 --topic xtandemtest)
6. Run the python file
