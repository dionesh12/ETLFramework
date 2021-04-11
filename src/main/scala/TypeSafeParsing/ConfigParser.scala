package TypeSafeParsing

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

object ConfigParser {
  val logger = LoggerFactory.getLogger(getClass.getName)

  /* This function is used to read the config File */
  def readConfig(configFileName: String): Config = {
    logger.info("Reading ConfFile")
    val configFile = ConfigFactory.load(configFileName)
    return configFile
  }

  def getAppName(configFile:Config):String = {
    logger.info("FileName")
    if (configFile.hasPath("header.appName")) {
      logger.info("Returning the appName")
      return configFile.getString("header.appName")
    } else {
      logger.error("As the config file doesnt have appName.Spark cant create session.Please mention the header.appName in config path")
      System.exit(1);
      ""
    }
  }
  def getFileOrTableLocation(configFile:Config,locale:String="source"):String = {
    logger.info("FileOrTableLocation information.")
    val partialLoc = "header.location-information"
    if (configFile.hasPath(partialLoc)) {
      logger.info("Location-information is specified")
      var filePath = ""
      if (configFile.hasPath(f"${partialLoc}.${locale}.location")) {
        filePath = configFile.getString(f"${partialLoc}.${locale}.location") match {
          case "hdfs" =>  {
            logger.info("HDFS specified")
            ""
          }
          case "local" => {
           logger.info("Local Path Specified")
            "file://"
          }
          case _ => {
            logger.info("Assuming hdfs path")
            ""
          }
        }
        if (configFile.hasPath(f"${partialLoc}.${locale}.path")) {
          logger.info("Obtaining the data location path")
          filePath += configFile.getString(f"${partialLoc}.${locale}.path")
        } else {
          logger.error(f"${partialLoc}.${locale}.path is not specified")
          System.exit(1)
        }
      } else {
        logger.info("Assuming hdfs Path has been sepcified as no location was provided")
        filePath = ""
      }
      logger.info("Will need to implement the table location standard")
      logger.info(s"File path being returned is ${filePath}")
      filePath
    }
    else {
      logger.error("The config file must contain either location-information or file-information")
      System.exit(1)
      ""
    }
  }



  /*Entry point to get appName,input filePath ,targetLocation and partitionColumns  if it exists exist partition list */
  def RequiredParameters(configFile:String) ={
    val configFileConf = readConfig(configFile)
    val appName = getAppName(configFileConf)
    val inputFilePath = getFileOrTableLocation(configFileConf)
    val targetFilePath = getFileOrTableLocation(configFileConf,"target")
    val schemaPath = getSchemaPath(configFileConf)
    val paramSet = (appName,inputFilePath,targetFilePath,schemaPath)
    paramSet
  }

  def getSchemaPath(configFile:Config):String = {
    if (configFile.hasPath("header.schema-path")) {
      return configFile.getString("header.schema-path")
    }
    ""
  }

  def checkPath(configFile:Config,path:String):Boolean = {
    logger.info("Checking Path")
    configFile.hasPath(path)
  }
  def getConfigValue(configFileName:String,path:String):Option[String] = {
    val configFile = readConfig(configFileName)
    if (checkPath(configFile,path)) {
      val dataObject = configFile.getString(path)
      logger.info(f"Obtaining DataObject ${dataObject}")
      Some(dataObject)

    } else {
      None
    }

  }

  def replaceNull(configFileName:String,columnName:String):Option[(String,Any)] = {
    logger.info("Here")
    getConfigValue(configFileName,"body.replace-null-values." + columnName) match {
      case t:Some[String] => Some(columnName,t.get.asInstanceOf[Any])
      case _ => {
        logger.error("Unable to Obtain Null Value.Exiting...")
        System.exit(1)
        None
      }
    }
  }

  def getPartitionColumns(configFileName:String):Option[Seq[String]] = {
    val configFile = readConfig("ecom.json")
    if (configFile.hasPath("header.location-information.target.partition-columns")) {
     val partitionColumns = configFile.getStringList("header.location-information.target.partition-columns").toArray().map(x=>(x.asInstanceOf[String])).toSeq
      Some(partitionColumns)
    } else {
      logger.error("Partion path columns do not exist.Exiting..")
      System.exit(1)
      None
    }

  }

  def getTimestampFormat(configFileName:String):Option[String] = {
    val configFile = readConfig(configFileName)
    if (configFile.hasPath("body.timestamp-pattern")) {
      val timeStampPattern = configFile.getString("body.timestamp-pattern")
      Some(timeStampPattern)
    } else {
      logger.error(" As time stamp pattern is not mentioned Exiting ..")
      None
    }
  }




}
