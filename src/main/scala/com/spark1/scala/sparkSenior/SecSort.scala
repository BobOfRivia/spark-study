package scala.com.spark1.scala.sparkSenior

/**
  * 实现二次排序(自定义)
  * 先按照第一列排序，如果第一列相同 则按照第二列
  */
class SecSort(val first:Int,val second:Int) extends Ordered[SecSort] with Serializable{

   def compare(that: SecSort): Int = {
    if(this.first != that.first) this.first - that.first
    else this.second - that.second
  }

  override def toString: String = {
    first+" "+second
  }

}

