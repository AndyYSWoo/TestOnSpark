package me.yongshang.cbfm.sparktest

import me.yongshang.cbfm.CBFM
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by yongshangwu on 2016/10/28.
  */
object SparkTest {
  val sparkContext = SparkSession
    .builder
    .appName("CBFM Test")
    .config("parquet.task.side.metadata", true)
    .config("parquet.enable.dictionary",false)
//    .config("spark.io.compression.codec", "lzf")
    .getOrCreate
  def main(args: Array[String]) {

    // config CBFM params
    CBFM.DEBUG = false
    CBFM.ON = true
    CBFM.desired_false_positive_probability_ = 0.1
    CBFM.setIndexedDimensions(Array("name", "age", "balance"))
    CBFM.reducedimensions = Array(3)
    createFile()
    query()
  }
  def createFile(): Unit ={
    val persons = Seq(
      Person("Jack", 21, 1000)
      , Person("Jason", 35, 5000)
      , Person("James", 40, 2000)
      , Person("Someone", 35, 3000)
      , Person("Customer#000000003", 35, 3000)
      , Person("MG9kdTD2WBHm", 35, 3000)
      , Person("11-719-748-3364", 35, 3000)
      , Person(" deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov", 35, 3000)
    )
    val personDF = sparkContext.createDataFrame(persons)
    personDF.show()
    personDF.createOrReplaceTempView("persons")
    val results = sparkContext.sql("SELECT * FROM persons where name='Jack' and age=21")
    results.show()
    personDF.write.parquet("persons.parquet")
  }
  def query(): Unit ={
    val persons = sparkContext.read.parquet("persons.parquet")
    persons.createOrReplaceTempView("persons")
    val results = sparkContext.sql("SELECT * FROM persons where name='Jack' and age=21 and balance=1000")
    results.show()
  }
}
case class Person(name: String, age: Int, balance: Double)

