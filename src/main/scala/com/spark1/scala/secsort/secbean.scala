package com.spark1.scala.secsort

class secbean(val first:Int,val second:Int) extends Ordered[secbean] with Serializable{

  override def compare(that: secbean): Int = {
    if(this.first == that.first){
      this.second - that.second
    }else{
      this.first-this.second
    }
  }

}
object secbean{
  def apply(first: Int,second: Int): secbean = new secbean(first,second)
}