package com.ibm.featureprocessing

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.sum
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Main extends App{

  val spark = SparkSession.builder
    .appName("MihuPraju")
    .master("local[*]")
    .getOrCreate()

  //prepareAttrs(spark)
  import spark.implicits._



}
