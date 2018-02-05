package actor.demo1

import scala.actors.Actor

/**
  * Created by lancerlin on 2018/2/2.
  *
  * 写个actor，可以不停地接收消息
  */
object ReceiveDemo {
  def main(args: Array[String]): Unit = {

  }

}

class MessageActor extends Actor{
  override def act(): Unit = {
    //在act()方法中加入了while (true) 循环，就可以不停的接收消息
    while (true) {
      //接收消息
      receive {
        case "start" => {
          println("开始接收消息")
          Thread.sleep(1000)
        }
        case "continue" => {
          println("接收消息中")
          Thread.sleep(1000)
        }
        case "stop" => {
          println("停止接收消息")
          Thread.sleep(1000)
        }
        //其他情况则退出否则会一直阻塞
        case _ => exit()
      }
    }
  }
}
object MessageActor {
  def main(args: Array[String]): Unit = {
    val ma = new MessageActor
    ma.start()
    //发送异步消息,发送异步消息(向actor发送start串)，发送完了继续往下走，但是一个actor对象接收到消息执行的过程是同步的按顺序执行
    //!,!?,!!三种发送方式
    ma ! "start"
    ma ! "continue"
    ma ! "stop"
    ma ! "break"
  }
}
