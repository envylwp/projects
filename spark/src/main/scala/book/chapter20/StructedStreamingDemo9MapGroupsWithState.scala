package book.chapter20

import org.apache.spark.sql.SparkSession

object StructedStreamingDemo9MapGroupsWithState {

  case class InputRow(user: String, timestamp: java.sql.Timestamp, activity: String)

  case class UserState(user: String,
                       var activity: String,
                       var start: java.sql.Timestamp,
                       var end: java.sql.Timestamp)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 5)
    val static = spark.read.json("/mnt/disk/IdeaProjects/projects/spark/data/activity-data")
    val streaming = spark
      .readStream
      .schema(static.schema)
      .option("maxFilesPerTrigger", 10)
      .json("/mnt/disk/IdeaProjects/projects/spark/data/activity-data")

    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    def updateUserStateWithEvent(state: UserState, input: InputRow): UserState = {
      if (Option(input.timestamp).isEmpty) {
        return state
      }
      if (state.activity == input.activity) {

        if (input.timestamp.after(state.end)) {
          state.end = input.timestamp
        }
        if (input.timestamp.before(state.start)) {
          state.start = input.timestamp
        }
      } else {
        if (input.timestamp.after(state.end)) {
          state.start = input.timestamp
          state.end = input.timestamp
          state.activity = input.activity
        }
      }
      state
    }

    import org.apache.spark.sql.streaming.{GroupStateTimeout,OutputMode,GroupState}
    def updateAcrossEvents(user:String,
      inputs:Iterator[InputRow],
      oldState:GroupState[UserState]):UserState={
      var state:UserState=if(oldState.exists)oldState.getelseUserState(user,
        "",
        new java.sql.Timestamp(6284160000000L),
        new java.sql.Timestamp(6284160L)
      )
      // we simply specify an old date that we can compare against and
      // immediately update based on the values in our data

      for(input<-inputs){
        state=updateUserStateWithEvent(state,input)
        oldState.update(state)
      }
      state
    }

  }
}
