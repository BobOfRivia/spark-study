package com.stock.offline.Extract.route2.Source2Hive

import com.stock.offline.Extract.route2.BeanUtils
import shapeless.T

class StockEncoder extends kafka.serializer.Encoder[T] {
  override def toBytes(t: T): Array[Byte] = {
    BeanUtils.ObjectToBytes(t)
  }
}
