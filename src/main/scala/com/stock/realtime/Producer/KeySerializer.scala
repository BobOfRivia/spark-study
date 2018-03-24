package com.stock.realtime.Producer

import java.util

import org.apache.kafka.common.serialization.Serializer

class KeySerializer extends Serializer[String]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ???

  override def serialize(topic: String, data: String): Array[Byte] = ???

  override def close(): Unit = ???
}
