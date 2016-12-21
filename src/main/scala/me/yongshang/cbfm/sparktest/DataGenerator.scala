package me.yongshang.cbfm.sparktest

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.sql.Date

import me.yongshang.cbfm.{CMDBF, MDBF, FullBitmapIndex, CBFM}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.thrift.protocol.TType

/**
  * Created by yongshangwu on 2016/11/1.
  */
object DataGenerator {
  val blockSize = 1 * 1024 * 1024
  val spark = SparkSession
    .builder
    .appName("Skip Test")
    .config("parquet.task.side.metadata", true)
    .config("parquet.enable.dictionary",false)
//    .config("spark.sql.join.preferSortMergeJoin", false)
    .config("spark.sql.parquet.compression.codec", "uncompressed")
    .getOrCreate

  def setUpCBFM(on: Boolean): Unit ={
    CBFM.DEBUG = false
    CBFM.ON = false
  }

  def setUpBitmapCBFM(on: Boolean, dimensions: Array[String], reduced: Array[Array[String]]): Unit ={
    FullBitmapIndex.ON = on;
    FullBitmapIndex.falsePositiveProbability = 0.1;
    FullBitmapIndex.setDimensions(dimensions, reduced)
  }

  def setUpMDBF(on: Boolean, dimensions: Array[String]): Unit ={
    MDBF.ON = on;
    MDBF.desiredFalsePositiveProbability = 0.1;
    MDBF.dimensions = dimensions;
  }

  def setUpCMDBF(on: Boolean, dimensions: Array[String]): Unit ={
    CMDBF.ON = on;
    CMDBF.desiredFalsePositiveProbability = 0.1;
    CMDBF.dimensions = dimensions;
  }

  def generateFile(dir: String): Unit ={
    val dirPath = dir

    val part = spark.sparkContext
      .textFile(dirPath+"part.tbl")
      .map(_.split("\\|"))
      .map(attrs => Part(attrs(0).toInt, attrs(1), attrs(2)
        , attrs(3), attrs(4), attrs(5).toInt
        , attrs(6), attrs(7).toDouble, attrs(8)))
    val partDF = spark.createDataFrame(part)
    partDF.write.parquet("part.parquet")
//
//    val supplier = spark.sparkContext
//      .textFile(dirPath+"supplier.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Supplier(attrs(0).toInt, attrs(1), attrs(2)
//        , attrs(3), attrs(4), attrs(5).toDouble
//        , attrs(6)))
//    val supplierDF = spark.createDataFrame(supplier)
//    supplierDF.write.parquet("supplier.parquet")
//
//    val partsupp = spark.sparkContext
//      .textFile(dirPath+"partsupp.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Partsupp(attrs(0).toInt, attrs(1).toInt, attrs(2).toInt
//        , attrs(3).toDouble, attrs(4)))
//    val partsuppDF = spark.createDataFrame(partsupp)
//    partsuppDF.write.parquet("partsupp.parquet")

//    val customer = spark.sparkContext
//      .textFile(dirPath+"customer.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Customer(attrs(0).toInt, attrs(1), attrs(2)
//        , attrs(3).toInt, attrs(4), attrs(5).toDouble
//        , attrs(6), attrs(7)))
//    val customerDF = spark.createDataFrame(customer)
//    customerDF.write.parquet("customer.parquet")
//
//    val orders = spark.sparkContext
//      .textFile(dirPath+"orders.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Orders(attrs(0).toInt, attrs(1).toInt, attrs(2)
//        , attrs(3).toDouble, Date.valueOf(attrs(4)), attrs(5)
//        , attrs(6), attrs(7), attrs(8)))
//    val ordersDF = spark.createDataFrame(orders)
//    ordersDF.write.parquet("orders.parquet")
//
    val lineitem = spark.sparkContext
      .textFile(dirPath+"lineitem.tbl")
      .map(_.split("\\|"))
      .map(attrs => Lineitem(attrs(0).toInt, attrs(1).toInt, attrs(2).toInt
        , attrs(3).toInt, attrs(4).toDouble, attrs(5).toDouble
        , attrs(6).toDouble, attrs(7).toDouble, attrs(8)
        , attrs(9), Date.valueOf(attrs(10)), Date.valueOf(attrs(11))
        , Date.valueOf(attrs(12)), attrs(13), attrs(14)
        , attrs(15)))
    val lineitemDF = spark.createDataFrame(lineitem)
    lineitemDF.createOrReplaceTempView("lineitem")
//    lineitemDF.write.parquet("lineitem.parquet")

//    val nation = spark.sparkContext
//      .textFile(dirPath+"nation.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Nation(attrs(0).toInt, attrs(1), attrs(2)
//        , attrs(3)))
//    val nationDF = spark.createDataFrame(nation)
//    nationDF.write.parquet("nation.parquet")
//
//    val region = spark.sparkContext
//      .textFile(dirPath+"region.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Region(attrs(0).toInt, attrs(1), attrs(2)))
//    val regionDF = spark.createDataFrame(region)
//    regionDF.write.parquet("region.parquet")
  }

  def readAndCreateTable(): Unit ={
    spark.read.parquet("part.parquet").createOrReplaceTempView("part")
//    spark.read.parquet("supplier.parquet").createOrReplaceTempView("supplier")
//    spark.read.parquet("partsupp.parquet").createOrReplaceTempView("partsupp")
    //    spark.read.parquet("customer.parquet").createOrReplaceTempView("customer")
    //    spark.read.parquet("orders.parquet").createOrReplaceTempView("orders")
//    spark.read.parquet("lineitem.parquet").createOrReplaceTempView("lineitem")
//    spark.read.parquet("nation.parquet").createOrReplaceTempView("nation")
//    spark.read.parquet("region.parquet").createOrReplaceTempView("region")
  }

  def main(args: Array[String]): Unit = {
    setUpCBFM(false)
    setUpBitmapCBFM(true, Array("ps_partkey", "ps_suppkey", "ps_supplycost"), Array(Array("ps_suppkey", "ps_supplycost")))
    generateFile(args(0))
  }
}



case class Part(p_partkey: Int, p_name: String, p_mfgr: String
                , p_brand: String, p_type: String, p_size: Int
                , p_container: String, p_retailprice: Double, p_comment: String)

case class Supplier(s_suppkey: Int, s_name: String, s_address: String
                    , s_nationkey: String, s_phone: String, s_acctbal: Double
                    , s_comment: String)

case class Partsupp(ps_partkey: Int, ps_suppkey: Int, ps_availqty: Int
                    , ps_supplycost: Double, ps_comment: String)

case class Customer(c_custkey: Int, c_name: String, c_address: String
                    , c_nationkey: Int, c_phone: String, c_acctbal: Double
                    , c_mksegment: String, c_comment: String)

case class Orders(o_orderkey: Int, o_custkey: Int, o_orderstatus: String
                  , o_totalprice: Double, o_orderdate: Date, o_orderpriority: String
                  , o_clerk: String, o_shippriority: String, o_comment: String)

case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int
                    , l_linenumber: Int, l_quantity: Double, l_extendedprice: Double
                    , l_discount: Double, l_tax: Double, l_returnflag: String
                    , l_linestatus: String, l_shipdate: Date, l_commitdate: Date
                    , l_receiptdate: Date, l_shipinstruct: String, l_shipmode: String
                    , l_comment: String)

case class Nation(n_nationkey: Int, n_name: String, n_regionkey: String
                  , n_comment: String)

case class Region(r_regionkey: Int, r_name: String, r_comment: String)