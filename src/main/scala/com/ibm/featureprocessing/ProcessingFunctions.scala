package com.ibm.featureprocessing

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import com.mongodb.spark._

class ProcessingFunctions {

  def processFeatures(spark: SparkSession,inputFeatures:String,transformations:String,datasetName:String) : String = {

    val uri: String = s"mongodb://mongouser:mongouser@mongodb/sampledb"
    // This piece is used for data loading in mongodb
    // val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\data\\apps\\projects\\datamicroservices\\openshift_spark\\src\\main\\resources\\WAFnUseCTelcoCustomerChurn.csv")
    // MongoSpark.save(df.write.option("collection", "churn_data").mode("overwrite"))
    //val spark = SparkSession.builder.master("spark://172.18.91.49:7077").config(sc.getConf).getOrCreate()
    val dfRead = spark.read.format("mongo").option("uri", "mongodb://mongouser:mongouser@mongodb/sampledb.churn_data").load()
    dfRead.select("customerID","gender")
    val sampleDf = dfRead.takeAsList(10).toString()
    //val readConfig = ReadConfig(Map("collection" -> "churn_data", Some(ReadConfig(spark)))
    //val customRdd = MongoSpark.load(spark readConfig)
    sampleDf
  }

  def getMongoData(spark: SparkSession,database:String,collection:String,columns:Array[String]): DataFrame ={
    // val uri: String = s"mongodb://mongouser:mongouser@mongodb/${database}.${collection}"
    val uri: String = s"mongodb://mongouser:mongouser@127.0.0.1:34000/${database}.${collection}"
    val dfRead = spark.read.format("mongo").option("uri", uri).load()
    val result = dfRead.select(columns.head, columns.tail: _*)
    result
  }

  def getMariaData(spark: SparkSession,database:String,tableName:String,columns:Array[String]): DataFrame ={
    val jdbcHostname = "mariadb"
    val jdbcPort = 3306
    val jdbcDatabase = database
    val jdbcUsername = "mariadbuser"
    val jdbcPassword = "mariadbuser"

    Class.forName("org.mariadb.jdbc.Driver")
    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    import java.sql.DriverManager
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

    val dfRead = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    val result = dfRead.select(columns.head, columns.tail: _*)
    result
  }


  def getMongoDataFromSQL(spark: SparkSession,database:String,collection:String,sql:String): DataFrame ={
    // val uri: String = s"mongodb://mongouser:mongouser@mongodb/${database}.${collection}"
    val uri: String = s"mongodb://mongouser:mongouser@127.0.0.1:34000/${database}.${collection}"
    val dfRead = spark.read.format("mongo").option("uri", uri).load()
    dfRead.createOrReplaceTempView(collection)
    val result = spark.sql(sql)
    result
  }

  def getMariaDataFromSQL(spark: SparkSession,database:String,tableName:String,sql:String): DataFrame ={
    //local
    //val jdbcHostname = "127.0.0.1"
    //val jdbcPort = 34006

    val jdbcHostname = "mariadb"
    val jdbcPort = 3306

    val jdbcDatabase = database
    val jdbcUsername = "mariadbuser"
    val jdbcPassword = "mariadbuser"

    Class.forName("org.mariadb.jdbc.Driver")
    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    import java.sql.DriverManager
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

    val dfRead = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    dfRead.createOrReplaceTempView(tableName)
    val result = spark.sql(sql)
    result
  }

  def prepareAttrs(spark:SparkSession,inputJson:String): String ={
    var dfMaria= spark.emptyDataFrame
    var dfMongo= spark.emptyDataFrame

    val processingFunctions = new ProcessingFunctions()
    //val inputJson:String = "{ \"attributes\": [ { \"desc\": \"Customer Identifier\", \"dbtype\": \"mongodb\", \"table\": \"cards\", \"column\": \"Customer_ID\", \"colT4mtn\": \"NA\" },{ \"desc\": \"Customer Identifier\", \"dbtype\": \"mariadb\", \"table\": \"demographics\", \"column\": \"Customer_ID\", \"colT4mtn\": \"NA\" }, { \"desc\": \"Business Date\", \"dbtype\": \"mongodb\", \"table\": \"cards\", \"column\": \"Business_Date\", \"colT4mtn\": \"NA\" }, { \"desc\": \"Card Type\", \"dbtype\": \"mongodb\", \"table\": \"cards\", \"column\": \"Card_Type\", \"colT4mtn\": \"NA\" }, { \"desc\": \"Paid Amount\", \"dbtype\": \"mongodb\", \"table\": \"cards\", \"column\": \"Paid_Amount\", \"colT4mtn\": \"NA\" }, { \"desc\": \"Date of Birth\", \"dbtype\": \"mariadb\", \"table\": \"demographics\", \"column\": \"DOB\", \"colT4mtn\": \"NA\" }, { \"desc\": \"Marital Status\", \"dbtype\": \"mariadb\", \"table\": \"demographics\", \"column\": \"Marital_Status\", \"colT4mtn\": \"NA\" }, { \"desc\": \"Postal Code\", \"dbtype\": \"mariadb\", \"table\": \"demographics\", \"column\": \"Postal_Code\", \"colT4mtn\": \"NA\" }, { \"desc\": \"Self Employed or Not\", \"dbtype\": \"mariadb\", \"table\": \"demographics\", \"column\": \"Self_Employed\", \"colT4mtn\": \"NA\" } ], \"transformations\": [ { \"DST4mtn\": \"Rolling Window\", \"params\": \"(Spend_AMT,10,Customer Identifier,Business Date)\", \"colName\": \"Carry_Over_Amt)\" } ], \"datasetname\": \"sample\" }"

    val jsonObject = new JSONObject(inputJson.trim())
    val keys = jsonObject.keys()

    var mongoDS = new JSONArray()
    var mongoTables = mutable.Set[String]()
    var mongoQueries = scala.collection.mutable.Map[String, String]()

    var mongoDatasets = new ArrayBuffer[Dataset[Row]]()
    var mariaDatasets = new ArrayBuffer[Dataset[Row]]()

    var mariaDS = new JSONArray()
    var mariaTables = mutable.Set[String]()
    var mariaQueries = scala.collection.mutable.Map[String, String]()

    println(inputJson)
    val attributesArray = jsonObject.getJSONArray("attributes")
    val len = attributesArray.length()
    for (i <- 1 to len) {
      val attrEntry = attributesArray.getJSONObject(i - 1)
      //println(attrEntry)
      if(attrEntry.getString("dbtype") == "mongodb"){
        mongoDS.put(attrEntry)
        mongoTables += attrEntry.getString("table")
      } else {
        mariaDS.put(attrEntry)
        mariaTables += attrEntry.getString("table")
      }
    }

    for(tableName <- mongoTables){
      var selectColumns = ""
      var firstEle:Boolean = true
      for (i <- 1 to mongoDS.length()) {
        val ele = mongoDS.getJSONObject(i - 1)
        if (ele.getString("table") == tableName) {
          if(!firstEle) {
            selectColumns += " , "
          }else{
            firstEle = false
          }
          if(ele.getString("colT4mtn") != "NA") {
            selectColumns += ele.getString("coolT4mtn") + "(" + ele.getString("column") + ") as " + ele.getString("column")
          }else
          {
            selectColumns += ele.getString("column")
          }
        }
      }

      mongoQueries(tableName) = "select " + selectColumns + " from " + tableName
      println(mongoQueries)
    }

    for(tableName <- mariaTables){
      var selectColumns = ""
      println(tableName)
      var firstEle:Boolean = true
      for (i <- 1 to mariaDS.length()) {
        val ele = mariaDS.getJSONObject(i - 1)
        if (ele.getString("table") == tableName) {
          if(!firstEle) {
            selectColumns += " , "
          }else{
            firstEle = false
          }
          if(ele.getString("colT4mtn") != "NA") {
            selectColumns += ele.getString("coolT4mtn") + "(" + ele.getString("column") + ") as " + ele.getString("column")
          }else
          {
            selectColumns += ele.getString("column")
          }
        }
      }

      mariaQueries(tableName) = "select " + selectColumns + " from " + tableName
      println(mariaQueries)
    }
    /*for( (tableName,sql) <- mongoQueries){
      mongoDatasets += processingFunctions.getMongoDataFromSQL(spark,"sampledb",tableName,sql)
    }
     */
    for( (tableName,sql) <- mariaQueries){
      mariaDatasets += processingFunctions.getMariaDataFromSQL(spark,"sampledb",tableName,sql)
    }
    if(mongoDatasets.length > 0){
      dfMongo = mongoDatasets(0)
      var i=1
      while(i < (mongoDatasets.length - 1) ){
        dfMongo = dfMongo.join(mongoDatasets(i),"Customer_ID")
        i+1
      }
    }
    if(mariaDatasets.length > 0){
      dfMaria = mariaDatasets(0)
      var i=1
      while(i < (mariaDatasets.length - 1) ){
        dfMaria = dfMaria.join(mariaDatasets(i),"Customer_ID")
        i+1
      }
    }
    dfMongo.printSchema()
    dfMaria.printSchema()
    var resulSet = spark.emptyDataFrame
    if(mongoDatasets.length == 0){
      resulSet = dfMaria
    }else if(mariaDatasets.length == 0){
      resulSet = dfMongo
    } else{
      resulSet = dfMongo.join(dfMaria,"Customer_ID")
    }

    MongoSpark.save(resulSet.write.option("uri", "mongodb://mongouser:mongouser@mongodb/sampledb.churn_data").option("collection", "mihika"))

    val transformationArray = jsonObject.getJSONArray("transformations")
    //println(mariaTables)
    //println(mariaDS)
    resulSet.take(5).toString()
  }

}
