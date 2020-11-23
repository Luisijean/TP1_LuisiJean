package jean.reader

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkReadWrite {
  def readData(inputPath: String, inputFormat: String) = {
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    /*inputFormat match {
      case "CSV" => sparkSession.read.csv(inputPath)
      case "parquet" => sparkSession.read.parquet(inputPath)
    }*/

    if (inputFormat == "CSV")
      sparkSession.read.option("header", true).csv(inputPath)
    else {
      sparkSession.read.option("header", true).parquet(inputPath)
    }

  }

  def writeData(df: DataFrame, outputPath: String, outputFormat: String, partitions: Seq[String], overwrite: Boolean): Unit = {
    if(outputFormat == "CSV"){
      if(overwrite)
        df.write.partitionBy(partitions:_*).mode(SaveMode.Overwrite).csv(outputPath)
      else{
        df.write.partitionBy(partitions:_*).csv(outputPath)
      }
    }
    else {
      if(overwrite)
        df.write.partitionBy(partitions:_*).mode(SaveMode.Overwrite).parquet(outputPath)
      else{
        df.write.partitionBy(partitions:_*).parquet(outputPath)
      }
    }
  }

}
