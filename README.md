# ProteinIdentificationGPU
Identification of proteins in DNA accelerated and executed on GPUs
PS: auto.create.topics.enable=true in congif/server.properties
PS2: Works and then does not anymore:::> set a small log offset "log.retention.bytes=1000" in config/Server.properties

Steps followed on ubuntu:
1. Started cassandra on localhost: Cassandra.bat and run the cql (connectCass.py) once.
2. Installed python's cassandra-driver and kafka-pyhon driver
3. Started zookeeper (bin/zookeeper-server-start.sh config/zookeeper.properties) #changed zookeper port to 2180 + config/server
4. Started kafka server (bin/kafka-server-start.sh config/server.properties)
5. Created a topic (bin/kafka-topics.sh --create --zookeeper localhost:2180 --replication-factor 1 --partitions 1 --topic xtandemtest)
6. Run the python file


Steps followed on windows (cygwin):
1. cd "C:\thesis\cassandra"  and then bin/cassandra.bat
2. cd "C:\thesis\kafka"      and then bin/zookeeper-server-start.sh config/zookeeper.properties
3. cd "C:\thesis\kafka"      and then bin/kafka-server-start.sh config/server.properties

ubuntu with daemon:
1. install zookeeper daemon               sudo apt-get install zookeeperd
2. start it                               go to "cd /usr/share/zookeeper/" and then:: sudo bin/zkServer.sh start
3. download and install kafka 2.10
4. start it                               bin/kafka-server-start.sh config/server.properties

