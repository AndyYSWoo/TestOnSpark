package me.yongshang.cbfm.sparktest

import java.io._
import java.nio.file.{Paths, Files}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import sys.process._

/**
  * Created by yongshangwu on 2016/12/8.
  */
object DnsQuery {
  val spark = DataGenerator.spark

  var index: String = null
  var dataSourceFolder: String = null
  var parquetPath: String = null
  var localDir: String = null
  var fs: FileSystem = null
  def main(args: Array[String]): Unit = {
    dataSourceFolder = "hdfs://server1:9000/data/dns/"
    index = args(1)
    parquetPath = args(2)+"dns.parquet"
    localDir = "/Users/yongshangwu/work/result/"+index+"/"

    // Dimensions, set them manually in parquet-hadoop, due to different jvms
    val dimensions = Array("sip", "dip", "nip")
    val reduced: Array[Array[String]] =  Array(Array("dip", "nip"))

//    loadAndWrite()
//
//    // Gather files, ugly hack
//    ("rm -rf "+localDir+"index-create-time").!
//    ("rm -rf "+localDir+"index-space").!
//    for(i <- Array(2,3,6,7)){
//      ("scp yongshangwu@server"+i+":/opt/record/"+index+"/index-create-time "+localDir+"ict"+i).!
//      (("cat "+localDir+"ict"+i) #>> new File(localDir+"index-create-time")).!
//      ("scp yongshangwu@server"+i+":/opt/record/"+index+"/index-space "+localDir+"is"+i).!
//      (("cat "+localDir+"is"+i) #>> new File(localDir+"index-space")).!
//    }

    spark.read.parquet(parquetPath).createOrReplaceTempView("dns")

    setupFS()
    queryAndRecord("1", 1)
    queryAndRecord("2", 1)
  }
  def loadAndWrite(): Unit ={
    val dns = spark.sparkContext
      .textFile(dataSourceFolder+"result")
      .map(_.split("\t"))
      .filter(tokens => NumberUtils.isNumber(tokens(4)) && NumberUtils.isNumber(tokens(5))
        && NumberUtils.isNumber(tokens(6)) && NumberUtils.isNumber(tokens(7))
        && NumberUtils.isNumber(tokens(9)) && NumberUtils.isNumber(tokens(10))
        && NumberUtils.isNumber(tokens(12)) && NumberUtils.isNumber(tokens(13))
        && NumberUtils.isNumber(tokens(14)) && NumberUtils.isNumber(tokens(15)))
      .map(tokens => Dns(tokens(0), tokens(1), tokens(2), tokens(3),
        tokens(4).toInt, tokens(5).toInt, tokens(6).toLong, tokens(7).toLong,
        tokens(8),tokens(9).toInt, tokens(10).toInt, tokens(11),
        tokens(12).toInt, tokens(13).toInt, tokens(14).toInt, tokens(15).toInt,
        tokens(16), tokens(17), tokens(18), tokens(19),
        tokens(20), tokens(21)))
    val dnsDF = spark.createDataFrame(dns)
    dnsDF.write.parquet(parquetPath)
  }

  def setupFS(): Unit ={
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://server1:9000")
    fs = FileSystem.get(conf)
  }

  def loadQueries(queryPath: String): Array[String] ={
    val file = fs.open(new Path(queryPath))
    val fileContent = new String(IOUtils.toByteArray(file))
    val queries = fileContent.split("====")
    queries
  }
  def queryAndRecord(query: String, count: Int): Unit ={
    // Writer
    val file = new File(localDir + "query"+query+"-time")
    if(file.exists()) file.delete()
    file.createNewFile()
    val pw = new PrintWriter(new FileWriter(file))

    // Query
    val queries = loadQueries(dataSourceFolder + "queries"+query+".sql")
    var totalTime = 0.0
    var i = 0
    for (i <- 0 until count) {
      val start = System.currentTimeMillis()
      val result = spark.sql(queries(i)).foreach(x => x)
      val time = System.currentTimeMillis() - start
      totalTime += time
      pw.write("query index "+i+": "+time + " ms. ")//+rowCount+"rows returned\n")
    }
    pw.write("=======\n")
    pw.write("\tavg: " + (totalTime / count)+"\n")

    pw.flush
    pw.close

    // Collect result
    collectResults(query, count)
    for(i <- Array(2,3,6,7)){
      ("scp /Users/yongshangwu/work/result/blank yongshangwu@server"+i+":/opt/record/"+index+"/index-load-time").!
      ("scp /Users/yongshangwu/work/result/blank yongshangwu@server"+i+":/opt/record/"+index+"/skip").!
    }
  }

  def collectResults(query: String, queryCount: Int): Unit ={
    var i = 1
    ("rm -rf "+localDir+"query"+query+"-index-load-time").!
    ("rm -rf "+localDir+"query"+query+"-skip").!

    for(i <- Array(2,3,6,7)){
      ("scp yongshangwu@server"+i+":/opt/record/"+index+"/index-load-time "+localDir+"query"+query+"-ilt"+i).!
      (("cat "+localDir+"query"+query+"-ilt"+i) #>> new File(localDir+"query"+query+"-index-load-time")).!

      ("scp yongshangwu@server"+i+":/opt/record/"+index+"/skip "+localDir+"query"+query+"-sk"+i).!
      (("cat "+localDir+"query"+query+"-sk"+i) #>> new File(localDir+"query"+query+"-skip")).!
    }
    // index load time
    var path = localDir + "query"+ query +"-index-load-time"
    var file = new File(path)
    if(file.exists()){
      val content = new String(Files.readAllBytes(Paths.get(path)))
      val lines = content.split("\n")
      var totalTime: Double = 0.0
      var count = 0
      for(line <- lines){
        if(line.startsWith("=")
          || line.startsWith("total:")
          || line.startsWith("per")){
          totalTime = 0.0
          count = 0
        }else{
          totalTime += Integer.valueOf(line.split(" ")(0))
          count += 1
        }
      }

      val pw = new PrintWriter(new FileWriter(file, true))
      pw.write("=======\n")
      pw.write("total: "+totalTime+" ms\n")
      pw.write("per query: "+(totalTime/queryCount)+" ms\n")
      pw.write("per block: "+(totalTime/count)+" ms\n")
      pw.flush
      pw.close
    }

    // skip
    path = localDir +"query" + query + "-skip"
    file = new File(path)
    if(file.exists()){
      val lines = new String(Files.readAllBytes(Paths.get(path))).split("\n")
      var line: String = null
      var totalBlocks = 0
      var skippedBlocks = 0
      var scannedRows = 0
      var skippedRows = 0
      for(line <- lines){
        if(line.startsWith("=")
          || line.startsWith("blocks:")
          || line.startsWith("rows:")){
          totalBlocks = 0
          skippedBlocks = 0
          scannedRows = 0
          skippedRows = 0
        }else{
          val tokens = line.split(" ")
          totalBlocks += Integer.valueOf(tokens(3))
          skippedBlocks += Integer.valueOf(tokens(5))
          scannedRows += Integer.valueOf(tokens(8))
          skippedRows += Integer.valueOf(tokens(11))
        }
      }
      val pw = new PrintWriter(new FileWriter(file, true))
      pw.write("=======\n")
      pw.write("blocks: total "+totalBlocks+", skipped "+skippedBlocks+", scanned "+(totalBlocks-skippedBlocks)+"\n")
      pw.write("rows: total "+(scannedRows+skippedRows)+", skipped "+skippedRows+", scanned "+scannedRows+"\n")
      pw.flush
      pw.close
    }

    // index create time
    path = localDir + "index-create-time"
    file = new File(path)
    if(file.exists()){
      val lines = new String(Files.readAllBytes(Paths.get(path))).split("\n")
      if(lines.contains("=======")) return
      var line: String = null
      var totalTime: Double = 0.0
      var count = 0
      for(line <- lines){
        if(line.startsWith("==")
          || line.startsWith("total")
          || line.startsWith("per")){
          totalTime = 0
          count = 0
        }else{
          totalTime += Integer.valueOf(line.split(" ")(0))
          count += 1
        }
      }
      val pw = new PrintWriter(new FileWriter(file, true))
      pw.write("=======\n")
      pw.write("total: "+totalTime+" ms\n")
      pw.write("per query: "+(totalTime/queryCount)+" ms\n")
      pw.write("per block: "+(totalTime/count)+" ms\n")
      pw.flush
      pw.close
    }

    // index space
    path = localDir + "index-space"
    file = new File(path)
    if(file.exists()){
      val lines = new String(Files.readAllBytes(Paths.get(path))).split("\n")
      var line: String = null
      var totalSpace: Double = 0.0
      var count = 0
      for(line <- lines){
        if(line.startsWith("==")
          || line.startsWith("total")
          || line.startsWith("per")){
          totalSpace = 0.0
          count = 0
        }else{
          totalSpace += java.lang.Double.valueOf(line.split(" ")(0))
          count += 1
        }
      }
      val pw = new PrintWriter(new FileWriter(file, true))
      pw.write("=======\n")
      pw.write("total: "+totalSpace+" MB\n")
      pw.write("per block: "+(totalSpace/count)+" MB\n")
      pw.flush
      pw.close
    }
  }

}
case class Dns(rip: String, sip: String, dip: String, nip: String,
               input: Int, output: Int, packets: Long, bytes: Long,
               time: String, sport: Int, dport: Int, flags: String,
               proto: Int, tos: Int, sas: Int, das: Int,
               scc: String, dcc: String, province: String, operator: String,
               spc: String, dpc: String)
