package CommonFuncs

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object SparkCommon {
    val logger = LoggerFactory.getLogger(getClass.getName)

  def getSparkSession(appName:String) = {
    val spark = SparkSession.builder().appName(appName).config("spark.master","local").enableHiveSupport().getOrCreate()
    spark
  }

  def readCSV(spark:SparkSession,location:String,schema :Option[StructType],headerInfo:String="true"):DataFrame= {
    val dataFrameRead1 = spark.read.format("csv").option("header",headerInfo)
    val dataFrameRead = schema.get match {
      case t:StructType =>  dataFrameRead1.schema(t)
      case _ =>   dataFrameRead1.option("inferSchema","true")
    }
    dataFrameRead.load(location)
  }

  def readParquet(spark:SparkSession,location:String,schema :Option[StructType],headerInfo:String="true"):DataFrame= {
    val dataFrameRead1 = spark.read.format("parquet").option("header",headerInfo)
    val dataFrameRead = schema.get match {
      case t:StructType =>  dataFrameRead1.schema(t)
      case _ =>   dataFrameRead1.option("inferSchema","true")
    }
    dataFrameRead.load(location)
  }
  def replaceStringNullInDataFrame(df:DataFrame,columnName:String,replaceMentValue:String):DataFrame = {
     df.na.fill(replaceMentValue,Seq(columnName))
      }


  def saveAsParquet(df:DataFrame,targetLocation:String,partitionColumns:Seq[String] = Seq()):Unit = {
      if (partitionColumns.length == 0) {
        df.write.format("parquet").option("header","true").mode(SaveMode.Append).save(targetLocation)
        logger.info("Saved")
      } else {
        df.write.format("parquet").option("header","true").partitionBy(partitionColumns:_*).mode(SaveMode.Append).save(targetLocation)
        logger.info("Saved")
      }

  }
}


