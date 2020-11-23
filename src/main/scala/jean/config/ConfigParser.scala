package jean.config

import scopt.{DefaultOParserSetup, OParserBuilder, OParserSetup}


case class ConfigParser(inputPath: String = "", inputFormat: String = "",
                        outputPath: String = "", outputFormat: String = "",
                        partitions: String = "", id: String = "")

object ConfigParser {
  private val setup: OParserSetup = new DefaultOParserSetup {
    override def showUsageOnError: Option[Boolean] = Some(true)
    override def errorOnUnknownArgument = false
  }
  import scopt.OParser
  val builder: OParserBuilder[ConfigParser] = OParser.builder[ConfigParser]

  val parser: OParser[Unit, ConfigParser] = {
    import builder._

    OParser.sequence(
      programName("GDPR Compliance"),

      opt[String]("inputPath")
        .required()
        .action((x, c) => c.copy(inputPath = x))
        .text("input path of my program"),

      opt[String]("outputPath")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("output path of my program"),

      opt[String]("inputFormat")
        .required()
        .action((x, c) => c.copy(inputFormat = x))
        .text("input format of my program"),

      opt[String]("outputFormat")
        .required()
        .action((x, c) => c.copy(outputFormat = x))
        .text("output format of my program"),

        opt[String]("partitions")
        .required()
        .action((x, c) => c.copy(partitions = x))
        .text("partitions of my program"),

      opt[String]("id")
        .required()
        .action((x, c) => c.copy(id = x))
        .text("client id")
    )
  }

  def parser(arguments: Array[String]): Option[ConfigParser] = {
    OParser.parse(ConfigParser.parser, arguments, ConfigParser(), setup)
  }

  def getConfigArgs(args: Array[String]): ConfigParser = {
    ConfigParser.parser(args) match {
      case Some(config) => config
      case _ => {
        print("cannot parse conf")
        sys.exit(1)
      }
    }
  }
}