package me.yongshang.cbfm.sparktest

import java.io.{FileWriter, PrintWriter, File}
import java.nio.file.{Paths, Files}

/**
  * Created by yongshangwu on 2016/12/14.
  */
object Util {
  def main(args: Array[String]) {
//    collectLoad("1")
//    collectCreate()
//    collectSpace()
    collectSkip
  }

  def collectLoad(query: String): Unit ={
    val localDir = "/Users/yongshangwu/Downloads/dns/cbfm/"
    // index load time
    for(i <- Array(2,3,6,7)){
      var path = localDir + "query"+ query +"-ilt"+i
      var file = new File(path)
      if(file.exists()){
        val content = new String(Files.readAllBytes(Paths.get(path)))
        val lines = content.split("\n")
        var totalTime: Double = 0.0
        var count = 0
        for(line <- lines){
          totalTime += Integer.valueOf(line.split(" ")(0))
          count += 1
        }

        val pw = new PrintWriter(new FileWriter(file, true))
        pw.write("=======\n")
        pw.write("total: "+totalTime+" ms\n")
        pw.write("per block: "+(totalTime/count)+" ms\n")
        pw.flush
        pw.close
      }
    }
  }
  def collectCreate(): Unit ={
    val path = "/Users/yongshangwu/Downloads/index-create-time"
    val file = new File(path)
    if(file.exists()){
      val lines = new String(Files.readAllBytes(Paths.get(path))).split("\n")
      if(lines.contains("=======")) return
      var line: String = null
      var totalTime: Double = 0.0
      var count = 0
      for(line <- lines){
        if(line.startsWith("==")
          || line.startsWith("total")
          || line.startsWith("per")
          || line.startsWith("block")){
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
      pw.write("per query: "+(totalTime/1)+" ms\n")
      pw.write("block count: "+count+"\n")
      pw.write("per block: "+(totalTime/count)+" ms\n")
      pw.flush
      pw.close
    }
  }
  def collectSpace(): Unit ={
    val path = "/Users/yongshangwu/Downloads/index-space"
    val file = new File(path)
    if(file.exists()){
      val lines = new String(Files.readAllBytes(Paths.get(path))).split("\n")
      var line: String = null
      var totalSpace: Double = 0.0
      var count = 0
      for(line <- lines){
        if(line.startsWith("==")
          || line.startsWith("total")
          || line.startsWith("per")
          || line.startsWith("block")){
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
      pw.write("block count: "+count+"\n")
      pw.write("per block: "+(totalSpace/count)+" MB\n")
      pw.flush
      pw.close
    }
  }
  def collectSkip: Unit ={
    val path = "/Users/yongshangwu/Downloads/dns/mdbf/query2-skip"
    val file = new File(path)
    if(file.exists()){
      val lines = new String(Files.readAllBytes(Paths.get(path))).split("\n")
      var line: String = null
      var totalBlocks = 0
      var skippedBlocks = 0
      var scannedRows: Long = 0
      var skippedRows: Long = 0
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

  }
}
