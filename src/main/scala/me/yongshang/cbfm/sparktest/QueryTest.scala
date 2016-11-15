package me.yongshang.cbfm.sparktest

import me.yongshang.cbfm.CBFM
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession

/**
  * Created by yongshangwu on 2016/11/3.
  */
object QueryTest {
  val spark = DataGenerator.spark
  def main(args: Array[String]) {
    DataGenerator.setUpCBFM(false)

    DataGenerator.setUpBitmapCBFM(true)

    DataGenerator.generateFile()

    readAndCreateTable()


    var query = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment\nfrom part, supplier, partsupp, nation, region\nwhere p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 9 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and ps_supplycost = ( \tselect \t\tmin(ps_supplycost) \tfrom \t\tpartsupp, \t\tsupplier, \t\tnation, \t\tregion \twhere \t\tp_partkey = ps_partkey \t\tand s_suppkey = ps_suppkey \t\tand s_nationkey = n_nationkey \t\tand n_regionkey = r_regionkey \t\tand r_name = 'ASIA' )\n"
    query = "select ps_supplycost from partsupp where ps_partkey=1"
//    query = "select l_extendedprice from lineitem where l_orderkey=10"
//    query = "SELECT s_acctbal, s_name, n_name, ps_partkey, s_address, s_phone, s_comment \nFROM (SELECT s_acctbal, s_name, n_name, s_address, s_phone, s_comment, s_suppkey \n        FROM nation, region, supplier \n        WHERE r_name='ASIA' AND n_regionkey=r_regionkey AND s_nationkey=n_nationkey), \n    (SELECT ps_partkey, ps_suppkey FROM partsupp WHERE ps_supplycost=( \n        SELECT min(ps_supplycost)\n        FROM partsupp, supplier, nation, region, (SELECT p_partkey FROM part WHERE p_size=9 AND p_type like '%BRASS')\n        WHERE r_name='ASIA' AND n_regionkey=r_regionkey AND s_nationkey=n_nationkey AND s_suppkey = ps_suppkey AND ps_partkey=p_partkey\n    ))\nWHERE ps_suppkey=s_suppkey"
    val start = System.currentTimeMillis();
    val results = spark.sql(query).foreach(x => x)
    println("==========query time: "+(System.currentTimeMillis()-start))
//    println(results.count())

//    stats()
  }

  def readAndCreateTable(): Unit ={
//    spark.read.parquet("part.parquet").createOrReplaceTempView("part")
//    spark.read.parquet("supplier.parquet").createOrReplaceTempView("supplier")
    spark.read.parquet("partsupp.parquet").createOrReplaceTempView("partsupp")
//    spark.read.parquet("lineitem.parquet").createOrReplaceTempView("lineitem")
//    spark.read.parquet("nation.parquet").createOrReplaceTempView("nation")
//    spark.read.parquet("region.parquet").createOrReplaceTempView("region")
  }

  def stats(): Unit ={
  }
}
