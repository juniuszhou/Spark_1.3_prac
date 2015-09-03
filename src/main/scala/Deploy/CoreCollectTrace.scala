/**
 * Created by junius on 14-8-18.
 */

// package name for deployed via this package.
package Deploy

import java.net.InetAddress



//this one successfully run at spark cluster.
object CoreCollectTrace {
  def CollectTrace(): Unit ={
    //get real hostname
    println(InetAddress.getLocalHost().getHostName())
    //get jvm name
    println(Runtime.getRuntime.exec("hostname"))

    //get stack trace
    val e: Exception = new Exception
    e.printStackTrace()
 }

  def main(args: Array[String]) {
    //val sc = new SparkContext("spark://172.16.32.1:7077", "CoreSort")
    CollectTrace()
  }
}


