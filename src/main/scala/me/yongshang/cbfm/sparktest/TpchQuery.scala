package me.yongshang.cbfm.sparktest

import java.io.{FileWriter, PrintWriter, File}
import java.nio.file.{Paths, Files}

import org.apache.parquet.filter2.compat.RowGroupFilter

/**
  * Created by yongshangwu on 2016/11/22.
  */
object TpchQuery {
  val spark = DataGenerator.spark
  var dataSourceFolder: String = null
  def main(args: Array[String]) {
    // File path config
    dataSourceFolder = args(0)
    RowGroupFilter.filePath = args(1)

    // Dimensions
    val dimensions = Array("p_size", "p_brand", "p_container")
    val reduced: Array[Array[String]] =  Array(Array("p_size", "p_container"))

    // Set up indexes
    val index = args(2)
    DataGenerator.setUpCBFM(false)
    DataGenerator.setUpBitmapCBFM(index.equals("cbfm"), dimensions, reduced)
    DataGenerator.setUpMDBF(index.equals("mdbf"), dimensions)
    DataGenerator.setUpCMDBF(index.equals("cmdbf"), dimensions)

    // Generate parquet files & register table
    DataGenerator.generateFile(dataSourceFolder)
    DataGenerator.readAndCreateTable()
    val queryCount = 1;
    queryAndRecord("2", queryCount)
    queryAndRecord("17", queryCount)
  }
  def loadQueries(queryPath: String): Array[String] ={
    val fileContent = new String(Files.readAllBytes(Paths.get(queryPath)));
    val queries = fileContent.split("====")
    return queries;
  }

  def queryAndRecord(query: String, count: Int): Unit ={
    RowGroupFilter.query = query;
    // Writer
    val resultFile: File = new File(RowGroupFilter.filePath + "query"+query+"-time")
    if (!resultFile.exists) resultFile.createNewFile
    val pw: PrintWriter = new PrintWriter(new FileWriter(resultFile, true))

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
    pw.write("\tavg: " + (totalTime / count))

    pw.flush
    pw.close
  }
}
