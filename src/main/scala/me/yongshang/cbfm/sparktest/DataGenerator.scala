package me.yongshang.cbfm.sparktest

import java.io.{File, FileWriter, PrintWriter}
import java.util.Date

import me.yongshang.cbfm.CBFM
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession

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
    .getOrCreate
  def main(args: Array[String]): Unit = {
    CBFM.DEBUG = false
    CBFM.ON = false
    CBFM.desired_false_positive_probability_ = 0.1
    CBFM.setIndexedDimensions(Array("c_custkey", "c_nationkey", "c_mksegment"))
    CBFM.reducedimensions = Array(6)

    val customer = spark.sparkContext
      .textFile("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/customer.tbl")
      .map(_.split("\\|"))
      .map(attrs => Customer(attrs(0).toInt, attrs(1), attrs(2)
        , attrs(3).toInt, attrs(4), attrs(5).toDouble
        , attrs(6), attrs(7))).collect()  // why .collect is needed?
    val customerDF = spark.createDataFrame(customer)
    customerDF.createOrReplaceTempView("customer")
    val start = System.currentTimeMillis()
    customerDF.write.parquet("customer.parquet")
    println("==========Time: "+(System.currentTimeMillis()-start))
    val readCustomer = spark.read.parquet("customer.parquet")
    readCustomer.createOrReplaceTempView("customer")
    val results = spark.sql("SELECT c_custkey FROM customer WHERE c_nationkey=15 AND c_mksegment='BUILDING'")
    println("==========result count: "+results.count())

    var totalBlocks: Long = 0;
    var skippedBlocks: Long = 0;
    val iter = SparkHadoopUtil.get.conf.iterator()
    while(iter.hasNext) {
      val entry = iter.next
      if (entry.getKey.startsWith(("parquet.cbfm.totalblocks."))) {
        totalBlocks = totalBlocks + SparkHadoopUtil.get.conf.getInt(entry.getKey, -1)
      }
      if (entry.getKey.startsWith("parquet.cbfm.skipblocks.")) {
        skippedBlocks = skippedBlocks + SparkHadoopUtil.get.conf.getInt(entry.getKey, -1)
      }
    }
    println("==========Total blocks: "+totalBlocks+", skipped blocks: "+skippedBlocks)
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

case class Orders(o_orderkey: Int, o_custkey: Int, o_orderstatus: Int
                  , o_totalprice: Double, o_orderdate: Date, o_orderpriority: String
                  , o_clerk: String, o_shippriority: String, o_comment: String)

case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int
                    , l_linenumber: Int, l_quantity: Double, l_extendedprice: Double
                    , l_discount: Double, l_tax: Double, l_returnflag: Double
                    , l_linestatus: Double, l_shipdate: Date, l_commitdate: Date
                    , l_receiptdate: Date, l_shipinstruct: String, l_shipmode: String
                    , l_comment: String)

case class Nation(n_nationkey: Int, n_name: String, n_regionkey: String
                  , n_comment: String)

case class Region(r_regionkey: Int, r_name: String, r_comment: String)