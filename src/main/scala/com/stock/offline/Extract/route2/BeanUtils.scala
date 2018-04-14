package com.stock.offline.Extract.route2

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream


object BeanUtils {
  /**
    * 对象转字节数组
    *
    * @param obj
    * @return
    */
  def ObjectToBytes(obj: Any): Array[Byte] = {
    var bytes: Array[Byte] = null
    var bo: ByteArrayOutputStream = null
    var oo: ObjectOutputStream = null
    try {
      bo = new ByteArrayOutputStream
      oo = new ObjectOutputStream(bo)
      oo.writeObject(obj)
      bytes = bo.toByteArray
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally try {
      if (bo != null) bo.close()
      if (oo != null) oo.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    bytes
  }

  /**
    * 字节数组转对象
    *
    * @param bytes
    * @return
    */
  def BytesToObject(bytes: Array[Byte]): Object = {
    var obj: Object = null
    var bi: ByteArrayInputStream = null
    var oi: ObjectInputStream = null
    try {
      bi = new ByteArrayInputStream(bytes)
      oi = new ObjectInputStream(bi)
      obj = oi.readObject
      obj
    } catch {
      case e: Exception =>
        e.printStackTrace()
        obj
    } finally try {
      if (bi != null) bi.close()
      if (oi != null) oi.close()
      obj
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }

  }
}

class BeanUtils private() {
}
