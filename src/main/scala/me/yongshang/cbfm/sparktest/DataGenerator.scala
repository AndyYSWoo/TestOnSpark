package me.yongshang.cbfm.sparktest

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.sql.Date

import me.yongshang.cbfm.{FullBitmapIndex, CBFM}
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
    .appName("TPC-H data generator")
    .config("parquet.task.side.metadata", true)
    .config("parquet.enable.dictionary",false)
//    .config("parquet.block.size", blockSize)
    .config("spark.sql.parquet.compression.codec", "uncompressed")
    .getOrCreate

  def setUpCBFM(on: Boolean): Unit ={
    CBFM.DEBUG = false
    CBFM.ON = on
    CBFM.desired_false_positive_probability_ = 0.1
    // customer
//        CBFM.setIndexedDimensions(Array("c_custkey", "c_nationkey", "c_mksegment"))
//        CBFM.reducedimensions = Array(6)

    // partsupp
    CBFM.setIndexedDimensions(Array("ps_partkey", "ps_suppkey", "ps_supplycost"))
    CBFM.reducedimensions = Array(3)

    // lineitem
//    CBFM.setIndexedDimensions(Array("l_orderkey", "l_partkey", "l_suppkey"))
//    CBFM.reducedimensions = Array(3)
  }

  def setUpBitmapCBFM(on: Boolean): Unit ={
    FullBitmapIndex.ON = on;
    FullBitmapIndex.falsePositiveProbability = 0.1;
    FullBitmapIndex.setDimensions(Array("ps_partkey", "ps_suppkey", "ps_supplycost"),
      Array(Array("ps_suppkey", "ps_supplycost")))
  }

  def generateFile(): Unit ={
    val dirPath = "/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/"
//
//    val part = spark.sparkContext
//      .textFile(dirPath+"part.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Part(attrs(0).toInt, attrs(1), attrs(2)
//        , attrs(3), attrs(4), attrs(5).toInt
//        , attrs(6), attrs(7).toDouble, attrs(8)))
//    val partDF = spark.createDataFrame(part)
//    partDF.write.parquet("part.parquet")
//
//    val supplier = spark.sparkContext
//      .textFile(dirPath+"supplier.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Supplier(attrs(0).toInt, attrs(1), attrs(2)
//        , attrs(3), attrs(4), attrs(5).toDouble
//        , attrs(6)))
//    val supplierDF = spark.createDataFrame(supplier)
//    supplierDF.write.parquet("supplier.parquet")

    val partsupp = spark.sparkContext
      .textFile(dirPath+"partsupp.tbl")
      .map(_.split("\\|"))
      .map(attrs => Partsupp(attrs(0).toInt, attrs(1).toInt, attrs(2).toInt
        , attrs(3).toDouble, attrs(4)))
    val partsuppDF = spark.createDataFrame(partsupp)
    partsuppDF.write.parquet("partsupp.parquet")

//    val customer = spark.sparkContext
//      .textFile(dirPath+"customer.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Customer(attrs(0).toInt, attrs(1), attrs(2)
//        , attrs(3).toInt, attrs(4), attrs(5).toDouble
//        , attrs(6), attrs(7)))
//    val customerDF = spark.createDataFrame(customer)
//    customerDF.write.parquet("customer.parquet")

//    val orders = spark.sparkContext
//      .textFile(dirPath+"orders.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Orders(attrs(0).toInt, attrs(1).toInt, attrs(2)
//        , attrs(3).toDouble, Date.valueOf(attrs(4)), attrs(5)
//        , attrs(6), attrs(7), attrs(8)))
//    val ordersDF = spark.createDataFrame(orders)
//    ordersDF.write.parquet("orders.parquet")

//    val lineitem = spark.sparkContext
//      .textFile(dirPath+"lineitem.tbl")
//      .map(_.split("\\|"))
//      .map(attrs => Lineitem(attrs(0).toInt, attrs(1).toInt, attrs(2).toInt
//        , attrs(3).toInt, attrs(4).toDouble, attrs(5).toDouble
//        , attrs(6).toDouble, attrs(7).toDouble, attrs(8)
//        , attrs(9), Date.valueOf(attrs(10)), Date.valueOf(attrs(11))
//        , Date.valueOf(attrs(12)), attrs(13), attrs(14)
//        , attrs(15)))
//    val lineitemDF = spark.createDataFrame(lineitem)
//    lineitemDF.write.parquet("lineitem.parquet")
//
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

  def main(args: Array[String]): Unit = {
    setUpCBFM(false)
    setUpBitmapCBFM(true)
    generateFile()
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