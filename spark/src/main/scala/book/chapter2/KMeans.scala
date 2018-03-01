package book.chapter2

import org.apache.spark.sql.SparkSession

object KMeans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions","5")

    val staticDataFrame = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/retail-data/by-day/*.csv")

    import org.apache.spark.sql.functions.{date_format, col}
    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week",date_format(col("InvoiceDate"),"EEEE"))
      .coalesce(5)
    preppedDataFrame.show(10)



    val trainDataFrame = preppedDataFrame
      .where("InvoiceDate <'2011-07-01'")
    val testDataFrame = preppedDataFrame
      .where("InvoiceDate >= '2011-07-01'")

    trainDataFrame.count()
    testDataFrame.count()

    import org.apache.spark.ml.feature.StringIndexer
    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")

    import org.apache.spark.ml.feature.OneHotEncoder
    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")

    import org.apache.spark.ml.feature.VectorAssembler
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice","Quantity","day_of_week_encoded"))
      .setOutputCol("features")

    import org.apache.spark.ml.Pipeline
    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer,encoder,vectorAssembler))

    val fittedPipeline = transformationPipeline.fit(trainDataFrame)
    val transformedTraining = fittedPipeline.transform(trainDataFrame)


    import org.apache.spark.ml.clustering.KMeans
    val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)
    val kmModel = kmeans.fit(transformedTraining)

    val transformedTest = fittedPipeline.transform(testDataFrame)
    kmModel.computeCost(transformedTest)

  }
}
