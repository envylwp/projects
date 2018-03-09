package structedstreaming

/**
  * Created by lancerlin on 2018/3/8. 
  */
object StringTest {
  def main(args: Array[String]): Unit = {

    val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE =
      """
        |Some data may have been lost because they are not available in Kafka any more; either the
        | data was aged out by Kafka or the topic may have been deleted before all the data in the
        | topic was processed. If you want your streaming query to fail on such cases, set the source
        | option "failOnDataLoss" to "true".
      """.stripMargin
  println(INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE)
  }

}
