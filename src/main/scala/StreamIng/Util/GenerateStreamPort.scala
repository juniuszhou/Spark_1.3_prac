package StreamIng.Util

import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random


/**
 * must put "\n" at the end of message. then it will send out .
 */
object GenerateStreamPort {
  def main (args: Array[String]) {
    val serverSocket = new ServerSocket(9999)

    val ran = new Random()
    while (true) {
      val socket = serverSocket.accept()

      new Thread() {
        override def run(): Unit = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(20)
            out.write(ran.nextInt(100).toString)
            out.write("\n")
            out.flush()
          }
          socket.close()
        }
      }.start()

      /*
      val stream = new PrintWriter(socket.getOutputStream, true)
      while (true){
        val value = ran.nextInt()
        println("&&&&  " + value)
        stream.write(value.toString)
        stream.flush()
        Thread.sleep(200)
      }*/


    }
  }
}
