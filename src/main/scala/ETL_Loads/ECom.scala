package ETL_Loads

import org.apache.spark.sql.functions.{regexp_extract, to_timestamp,year,lpad,month,dayofmonth}
import org.slf4j.LoggerFactory

object ECom {
  val logger = LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val (appName,
         inputPath,
         targetPath,
         schemaPath) = TypeSafeParsing.ConfigParser.RequiredParameters("ecom.json")

    logger.info("Schema  Generating")
    val schema = CommonFuncs.SchemaSet.getSchema(schemaPath)
    logger.info("Schema  Generated")


    logger.info("Spark Session Generating")
    val spark = CommonFuncs.SparkCommon.getSparkSession(appName)
    logger.info("Spark Session Generated")


    logger.info("Generate a DataFrame")
    val ecomBaseDF = CommonFuncs.SparkCommon.readCSV(spark,inputPath,schema).limit(1)  // Test
    logger.info("Generated a DataFrame")

    logger.info("Replace Null Value for Categor_Code")
    val (columnName,replacementValue) = TypeSafeParsing.ConfigParser.replaceNull("ecom.json","category_code").get
    logger.info("Applying the replacement to the dataframe")
    val ecomBaseReplacedDF = CommonFuncs.SparkCommon.replaceStringNullInDataFrame(ecomBaseDF,columnName,replacementValue.asInstanceOf[String])
    logger.info("Applied Replacement Values")

    /* Custom Logic */
    val timeStampFormat = TypeSafeParsing.ConfigParser.getTimestampFormat("ecom.json").get
    val ecomnewEventDF =  ecomBaseReplacedDF.withColumn("event_time_formatted",to_timestamp(regexp_extract(
      ecomBaseReplacedDF.col("event_time"),timeStampFormat,0)))
      .drop(ecomBaseReplacedDF.col("event_time"))
      .withColumnRenamed("event_time_formatted","event_time")

    val ecomSavedDF =      ecomnewEventDF.withColumn("year",lpad(year(ecomnewEventDF.col("event_time")),2,"0"))
      .withColumn("month",lpad(month(ecomnewEventDF.col("event_time")),2,"0"))
      .withColumn("day",lpad(dayofmonth(ecomnewEventDF.col("event_time")),2,"0"))

    /* Custom Logic */

    logger.info("Store the Data")
    val parquetColumns = TypeSafeParsing.ConfigParser.getPartitionColumns("ecom.json").get
    CommonFuncs.SparkCommon.saveAsParquet(ecomSavedDF,targetPath,parquetColumns)







  }
}
