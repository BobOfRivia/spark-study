package com.stock.realtime.Producer

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class StockPartitioner extends  Partitioner{

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = ???

  override def close(): Unit = ???

  override def configure(configs: util.Map[String, _]): Unit = ???
}
