#!/bin/bash
#################################cd
#cd ap_thesis/kafka_2.11-0.11.0.1/
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic topic_mgf
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic uidMatches
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic pairs1
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic pairs2
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic pairs3
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic pairs4
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic pairs5
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic pairs6
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic pairs7
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic pairs8
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic scores
sudo bin/kafka-topics.sh --delete --zookeeper localhost:2180 --topic results
cd ap_logs/
sudo rm -rf *
cd ..
sudo bin/kafka-server-start.sh config/server.properties


