package com.esgi.victor

import com.esgi.victor.bean.Configuration
import com.google.gson.Gson
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.{BufferedSource, Source}

object Main {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("cc_spark").getOrCreate()

  def main(args: Array[String]): Unit ={

    val conf: Configuration = readConf(args(0))
    val data: DataFrame = Reader.run(conf.source)
    data.show()
    Storer.store(data, conf.target + "/meteo.parquet")

  }

  def readConf(confPath: String): Configuration ={

    val gson: Gson = new Gson
    val file: BufferedSource = Source.fromFile(confPath)
    val conf: Configuration = gson.fromJson(file.mkString, classOf[Configuration])
    file.close()
    conf

  }

}
