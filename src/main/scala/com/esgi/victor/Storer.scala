package com.esgi.victor

import org.apache.spark.sql.{DataFrame, SparkSession}

object Storer {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("cc_spark").getOrCreate()

  def store(df: DataFrame, targetPath: String): Unit ={

    df.write
      .format("org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2")
      .partitionBy("id_station")
      .mode("append")
      .save(targetPath)

  }

}
