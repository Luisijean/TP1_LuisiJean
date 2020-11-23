package jean.reader

import jean.config.ConfigParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._


object MainProgram {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val configCli = ConfigParser.getConfigArgs(args)
    println(configCli.inputPath)
    println(configCli.outputPath)

    val df = SparkReadWrite.readData(configCli.inputPath, configCli.inputFormat)

    val dfWithoutId = df.filter(col("ID") =!= configCli.id)

    SparkReadWrite.writeData(
      dfWithoutId, configCli.outputPath, configCli.outputFormat, Seq(configCli.partitions), overwrite = true
    )

  }
}
