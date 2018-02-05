package actor.demo1

import scala.actors.Actor

/**
  * Created by lancerlin on 2018/2/2. 
  */
object Demo1 {
  def main(args: Array[String]): Unit = {
      new ActorWorkder1().start()
      new ActorWorkder2().start()
  }

}

class ActorWorkder1 extends Actor{
  override def act(): Unit = {

    for(i <- 1 to 10){
      println(s"actor-worker1:$i")
      Thread.sleep(400)
    }
  }
}

class ActorWorkder2 extends Actor{
  override def act(): Unit = {

    for(i <- 1 to 10){
      println(s"actor-worker2:$i")
      Thread.sleep(500)
    }
  }
}