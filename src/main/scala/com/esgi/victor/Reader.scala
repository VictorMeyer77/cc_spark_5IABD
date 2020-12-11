package com.esgi.victor

import com.esgi.victor.bean.DepartementOrd
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Reader {

  val spark: SparkSession = SparkSession
            .builder()
            .appName("cc_spark").getOrCreate()


  def readTable(path: String): DataFrame ={

    spark.read
      .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
      .option("header", value = true)
      .option("delimiter", ";")
      .load(path)

  }

  def splitDepartementOrd(departements: DataFrame): DataFrame ={

    import spark.implicits._
    val ord = departements.filter(length(col("geom_x_y")) > 5)
      .map(dep => new DepartementOrd(dep.getAs[String]("CODE INSEE"),
      dep.getAs[String]("geom_x_y").split(",")(0),
      dep.getAs[String]("geom_x_y").split(",")(1)))
      .withColumnRenamed("codeInsee", "CODE INSEE")
      .withColumnRenamed("latitude", "Latitude")
      .withColumnRenamed("longitude", "Longitude")

    departements.join(ord, departements("CODE INSEE") === ord("CODE INSEE"), "inner")

  }

  def formatOrd(df: DataFrame): DataFrame ={

    import spark.implicits._
    df.withColumn("ord",
      concat(substring(col("Latitude"), 0, 3).as[String],
      substring(col("Longitude"), 0, 3).as[String]))

  }

  def formatFullDate(fullData: DataFrame): DataFrame ={

    fullData.select("date", "Code Dept", "Population", "t", "numer_sta")
      .withColumnRenamed("date", "jour")
      .withColumnRenamed("Code Dept", "departement")
      .withColumnRenamed("Population", "population")
      .withColumnRenamed("t", "temperature")
      .withColumnRenamed("numer_sta", "id_station")

  }

  def run(sourcePath: String): DataFrame ={

    // read
    val communes: DataFrame = readTable(sourcePath + "/Communes.csv")
    val departements: DataFrame = readTable(sourcePath + "/code-insee-postaux-geoflar.csv")
    val stationMeteos: DataFrame = readTable(sourcePath + "/postesSynop.txt")
    val synop: DataFrame = readTable(sourcePath + "/synop.2020120512.txt")

    // format

    val townWithDep = communes.join(departements, communes("DEPCOM") === departements("CODE INSEE"), "inner")
    val townDepOrd = formatOrd(splitDepartementOrd(townWithDep))
    val posteOrd = formatOrd(stationMeteos)
    val townDepPoste = townDepOrd.join(posteOrd, posteOrd("ord") === townDepOrd("ord"), "inner")
    val fullData = townDepPoste.join(synop, synop("numer_sta") === townDepPoste("ID"), "inner")

    formatFullDate(fullData)

  }
}
