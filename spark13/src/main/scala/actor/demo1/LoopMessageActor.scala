package actor.demo1

import scala.actors.Actor

/**
  * Created by lancerlin on 2018/2/2. 
  */
class LoopMessageActor extends Actor{
  override def act(): Unit = {
    loop {
      react {
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
object LoopMessageActor {
  def main(args: Array[String]): Unit = {
    val lma = new LoopMessageActor
    lma.start()
    lma ! "start"
    lma ! "continue"
    lma ! "continue"
    lma ! "continue"
    lma ! "stop"
    lma ! "break"
  }
}