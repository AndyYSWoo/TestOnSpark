package me.yongshang.cbfm.sparktest

import java.io._
import java.nio.file.{Paths, Files}
import java.sql.Date

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.parquet.filter2.compat.RowGroupFilter
import org.apache.spark.sql.DataFrame

/**
  * Created by yongshangwu on 2016/11/22.
  */
object TpchQuery {
  val spark = DataGenerator.spark
  var dataSourceFolder: String = null
  var fs: FileSystem = null
  def main(args: Array[String]) {
    // File path config
    dataSourceFolder = args(0)
    RowGroupFilter.filePath = args(1)

    // Dimensions
    val dimensions = Array("p_type", "p_brand", "p_container")
    val reduced: Array[Array[String]] =  Array(Array("p_type", "p_container"))

    // Load & denormalize tables
    val denormalized = loadDataAndDenormalize()

    // Set up indexes
    val index = args(2)
    DataGenerator.setUpCBFM(false)
    DataGenerator.setUpBitmapCBFM(index.equals("cbfm"), dimensions, reduced)
    DataGenerator.setUpMDBF(index.equals("mdbf"), dimensions)
    DataGenerator.setUpCMDBF(index.equals("cmdbf"), dimensions)

    // Write out with index
    val parquetDir = args(3)
    val parquetFile = parquetDir+"denormalized.parquet"
    denormalized.write.parquet(parquetFile)

    // Read denormalized table & query
    spark.read.parquet(parquetFile).createOrReplaceTempView("denormalized")

    // Query
    setupFS()
    val queryCount = 1
    queryAndRecord("8", queryCount)
    queryAndRecord("17", queryCount)
  }

  def setupFS(): Unit ={
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://tina:9000")
    fs = FileSystem.get(conf)
  }

  def loadDataAndDenormalize(): DataFrame ={
    val part = spark.sparkContext
      .textFile(dataSourceFolder+"part.tbl")
      .map(_.split("\\|"))
      .map(attrs => Part(attrs(0).toInt, attrs(1), attrs(2)
        , attrs(3), attrs(4), attrs(5).toInt
        , attrs(6), attrs(7).toDouble, attrs(8)))
    val partDF = spark.createDataFrame(part)
    partDF.createOrReplaceTempView("part")

    val supplier = spark.sparkContext
      .textFile(dataSourceFolder+"supplier.tbl")
      .map(_.split("\\|"))
      .map(attrs => Supplier(attrs(0).toInt, attrs(1), attrs(2)
        , attrs(3), attrs(4), attrs(5).toDouble
        , attrs(6)))
    val supplierDF = spark.createDataFrame(supplier)
    supplierDF.createOrReplaceTempView("supplier")

    val partsupp = spark.sparkContext
      .textFile(dataSourceFolder+"partsupp.tbl")
      .map(_.split("\\|"))
      .map(attrs => Partsupp(attrs(0).toInt, attrs(1).toInt, attrs(2).toInt
        , attrs(3).toDouble, attrs(4)))
    val partsuppDF = spark.createDataFrame(partsupp)
    partsuppDF.createOrReplaceTempView("partsupp")

    val customer = spark.sparkContext
      .textFile(dataSourceFolder+"customer.tbl")
      .map(_.split("\\|"))
      .map(attrs => Customer(attrs(0).toInt, attrs(1), attrs(2)
        , attrs(3).toInt, attrs(4), attrs(5).toDouble
        , attrs(6), attrs(7)))
    val customerDF = spark.createDataFrame(customer)
    customerDF.createOrReplaceTempView("customer")

    val orders = spark.sparkContext
      .textFile(dataSourceFolder+"orders.tbl")
      .map(_.split("\\|"))
      .map(attrs => Orders(attrs(0).toInt, attrs(1).toInt, attrs(2)
        , attrs(3).toDouble, Date.valueOf(attrs(4)), attrs(5)
        , attrs(6), attrs(7), attrs(8)))
    val ordersDF = spark.createDataFrame(orders)
    ordersDF.createOrReplaceTempView("orders")

    val lineitem = spark.sparkContext
      .textFile(dataSourceFolder+"lineitem.tbl")
      .map(_.split("\\|"))
      .map(attrs => Lineitem(attrs(0).toInt, attrs(1).toInt, attrs(2).toInt
        , attrs(3).toInt, attrs(4).toDouble, attrs(5).toDouble
        , attrs(6).toDouble, attrs(7).toDouble, attrs(8)
        , attrs(9), Date.valueOf(attrs(10)), Date.valueOf(attrs(11))
        , Date.valueOf(attrs(12)), attrs(13), attrs(14)
        , attrs(15)))
    val lineitemDF = spark.createDataFrame(lineitem)
    lineitemDF.createOrReplaceTempView("lineitem")

    val nation = spark.sparkContext
      .textFile(dataSourceFolder+"nation.tbl")
      .map(_.split("\\|"))
      .map(attrs => Nation(attrs(0).toInt, attrs(1), attrs(2)
        , attrs(3)))
    val nationDF = spark.createDataFrame(nation)
    nationDF.createOrReplaceTempView("nation")

    val region = spark.sparkContext
      .textFile(dataSourceFolder+"region.tbl")
      .map(_.split("\\|"))
      .map(attrs => Region(attrs(0).toInt, attrs(1), attrs(2)))
    val regionDF = spark.createDataFrame(region)
    regionDF.createOrReplaceTempView("region")

    val joinQuery =
      "SELECT * FROM " +
        "part LEFT OUTER JOIN lineitem ON p_partkey=l_partkey " +
        "JOIN supplier ON l_suppkey=s_suppkey " +
        "JOIN orders ON o_orderkey=l_orderkey " +
        "JOIN customer ON o_custkey=c_custkey"
    spark.sql(joinQuery).createOrReplaceTempView("denormalized")
    spark.table("denormalized")
  }

  def loadQueries(queryPath: String): Array[String] ={
    val file = fs.open(new Path(queryPath))
    val fileContent = new String(IOUtils.toByteArray(file));
    val queries = fileContent.split("====")
    queries
  }

  def queryAndRecord(query: String, count: Int): Unit ={
    // Writer
    val timePath = new Path(RowGroupFilter.filePath + "query"+query+"-time")
    val output = fs.create(timePath)
    val pw = new PrintWriter(output)

    // Query
    val queries = loadQueries(dataSourceFolder + "queries"+query+".sql")
    var totalTime = 0.0
    var i = 0
    for (i <- 0 until count) {
      val start = System.currentTimeMillis()
      val result = spark.sql(queries(i))
      val rowCount = result.count();
      val time = System.currentTimeMillis() - start
      totalTime += time
      pw.write("query index "+i+": "+time + " ms. "+rowCount+"rows returned\n")
    }
    pw.write("=======\n")
    pw.write("\tavg: " + (totalTime / count)+"\n")

    pw.flush
    pw.close

    // Collect result
    collectResults(query, count)
  }

  def collectResults(query: String, queryCount: Int): Unit ={
    // index load time
    var path = new Path(RowGroupFilter.filePath + "query" + query + "-index-load-time")
    if(fs.exists(path)){
      val in = fs.open(path)
      val content = new String(IOUtils.toByteArray(in))
      in.close
      val lines = content.split("\n")
      var line: String = null
      var totalTime: Double = 0.0
      var count = 0
      for(line <- lines){
        totalTime += Integer.valueOf(line.split(" ")(0))
        count += 1
      }

      val pw = new PrintWriter(fs.append(path))
      pw.write("=======\n")
      pw.write("total: "+totalTime+" ms\n")
      pw.write("per query: "+(totalTime/queryCount)+" ms\n")
      pw.write("per block: "+(totalTime/count)+" ms\n")
      pw.flush
      pw.close
    }

    // skip
    path = new Path(RowGroupFilter.filePath + "query" + query + "-skip")
    if(fs.exists(path)){
      val in = fs.open(path)
      val lines = new String(IOUtils.toByteArray(in)).split("\n")
      in.close
      var line: String = null
      var totalBlocks = 0
      var skippedBlocks = 0
      var scannedRows = 0
      var skippedRows = 0
      for(line <- lines){
        val tokens = line.split(" ")
        totalBlocks += Integer.valueOf(tokens(3))
        skippedBlocks += Integer.valueOf(tokens(5))
        scannedRows += Integer.valueOf(tokens(8))
        skippedRows += Integer.valueOf(tokens(11))
      }
      val pw = new PrintWriter(fs.append(path))
      pw.write("=======\n")
      pw.write("blocks: total "+totalBlocks+", skipped "+skippedBlocks+", scanned "+(totalBlocks-skippedBlocks)+"\n")
      pw.write("rows: total "+(scannedRows+skippedRows)+", skipped "+skippedRows+", scanned "+scannedRows+"\n")
      pw.flush
      pw.close
    }

    // index create time
    path = new Path(RowGroupFilter.filePath + "index-create-time")
    if(fs.exists(path)){
      val in = fs.open(path)
      val lines = new String(IOUtils.toByteArray(in)).split("\n")
      in.close
      if(lines.contains("=======")) return // collect index create time & space only once
      var line: String = null
      var totalTime: Double = 0.0
      var count = 0
      for(line <- lines){
        totalTime += Integer.valueOf(line.split(" ")(0))
        count += 1
      }
      val pw = new PrintWriter(fs.append(path))
      pw.write("=======\n")
      pw.write("total: "+totalTime+" ms\n")
      pw.write("per query: "+(totalTime/queryCount)+" ms\n")
      pw.write("per block: "+(totalTime/count)+" ms\n")
      pw.flush
      pw.close
    }

    // index space
    path = new Path(RowGroupFilter.filePath + "index-space")
    if(fs.exists(path)){
      val in = fs.open(path)
      val lines = new String(IOUtils.toByteArray(in)).split("\n")
      in.close
      var line: String = null
      var totalSpace: Double = 0.0
      var count = 0
      for(line <- lines){
        totalSpace += java.lang.Double.valueOf(line.split(" ")(0))
        count += 1
      }
      val pw = new PrintWriter(fs.append(path))
      pw.write("=======\n")
      pw.write("total: "+totalSpace+" MB\n")
      pw.write("per block: "+(totalSpace/count)+" MB\n")
      pw.flush
      pw.close
    }
  }
}
