#!/bin/bash
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/zookeeper-server-stop.sh /home/patrikar/ap_thesis/kafka_2.11-1.0.0/config/zookeeper.properties &
sudo rm -rf /tmp/zookeeper/*
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/zookeeper-server-start.sh /home/patrikar/ap_thesis/kafka_2.11-1.0.0/config/zookeeper.properties &
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic topic_mgf
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic numofmatches
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic pairs2
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic pairs3
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic pairs4
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic pairs5
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic pairs6
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic pairs7
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic pairs8
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic pairs1
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic scores
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-topics.sh --delete --zookeeper 192.168.145.20:2181 --topic results
sudo rm -rf /home/patrikar/ap_thesis/kafka_2.11-1.0.0/ap_logs/*
sudo /home/patrikar/ap_thesis/kafka_2.11-1.0.0/bin/kafka-server-start.sh /home/patrikar/ap_thesis/kafka_2.11-1.0.0/config/server.properties

