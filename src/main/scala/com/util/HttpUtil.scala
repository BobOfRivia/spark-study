package com.util

/**
  * Created by JACK on 2018/3/9.
  */
object HttpUtil {
  def get(url:String,conntectTimeout:Int = 5000,readTimeout:Int = 5000,requestMethod : String = "GET",charset:String): String ={
    import java.net.{URL , HttpURLConnection}
    val conntection = (new URL(url)).openConnection().asInstanceOf[HttpURLConnection]
    conntection.setConnectTimeout(conntectTimeout)
    conntection.setReadTimeout(readTimeout)
    conntection.setRequestMethod(requestMethod)
    scala.io.Source.fromInputStream(conntection.getInputStream,charset).mkString
  }
}
