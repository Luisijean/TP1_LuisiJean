package jean.reader

import jean.config.ConfigParser
import org.apache.log4j.{Level, Logger}


object SampleProgram {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val configCli = ConfigParser.getConfigArgs(args)
    println(configCli.inputPath)
    println(configCli.outputPath)

    val df = SparkReadWrite.readData(configCli.inputPath, configCli.inputFormat)

    val selectedDf = df.select("Region", "Country")
    SparkReadWrite.writeData(
      selectedDf, configCli.outputPath, configCli.outputFormat, Seq(configCli.partitions), overwrite = false
    )

  }
}

