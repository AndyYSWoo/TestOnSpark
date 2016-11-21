package me.yongshang.cbfm.sparktest

import me.yongshang.cbfm.CBFM
import org.apache.parquet.filter2.compat.RowGroupFilter
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession

/**
  * Created by yongshangwu on 2016/11/3.
  */
object QueryTest {
  val spark = DataGenerator.spark
  def main(args: Array[String]) {
    // file config
    val dataSourceFolder = args(0)
    RowGroupFilter.filePath = args(1)

    // dimensions
    val dimensions = Array(
//      "ps_partkey", "ps_suppkey", "ps_supplycost"
//      "ps_partkey"
      "p_size", "p_brand", "p_container"
//      "p_brand"
    )
    val reduced: Array[Array[String]] =  Array(
//      Array("ps_suppkey", "ps_supplycost")
//      Array()
      Array("p_size", "p_container")
    )

    // set up indexes
    val index = args(2)
    DataGenerator.setUpCBFM(false)
    DataGenerator.setUpBitmapCBFM(index.equals("cbfm"), dimensions, reduced)
    DataGenerator.setUpMDBF(index.equals("mdbf"), dimensions)

    // generate file & create table
    DataGenerator.generateFile(dataSourceFolder)
    readAndCreateTable()

    // query
    var query = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment\nfrom part, supplier, partsupp, nation, region\nwhere p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 9 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and ps_supplycost = ( \tselect \t\tmin(ps_supplycost) \tfrom \t\tpartsupp, \t\tsupplier, \t\tnation, \t\tregion \twhere \t\tp_partkey = ps_partkey \t\tand s_suppkey = ps_suppkey \t\tand s_nationkey = n_nationkey \t\tand n_regionkey = r_regionkey \t\tand r_name = 'ASIA' )\n"
//    query = "select ps_supplycost from partsupp where ps_partkey=1"
//    query = "select ps_supplycost from partsupp where ps_comment=', even theodolites. regular, final theodolites eat after the carefully pending foxes. furiously regular deposits sleep slyly. carefully bold realms above the ironic dependencies haggle careful'"
//    query = "SELECT p_name, ps_supplycost FROM part, partsupp WHERE ps_suppkey=1 AND ps_partkey=p_partkey"
    query = "SELECT p_retailprice FROM part WHERE p_size=7"
    spark.sql(query).explain()

    val start = System.currentTimeMillis();
    val results = spark.sql(query)
    results.show(10)
    println("==========query time: "+(System.currentTimeMillis()-start))
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
}
