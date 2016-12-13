package me.yongshang.cbfm.sparktest

import java.io.{FileWriter, PrintWriter, File}
import java.nio.file.{Paths, Files}

import sys.process._
/**
  * Created by yongshangwu on 2016/12/9.
  */
object ShellTest {
  def main(args: Array[String]) {
    val index = "cbfm"
    val localDir = "/Users/yongshangwu/work/result/" + index + "/"
    val query = "17"
    val queryCount = 1


    ("rm -rf "+localDir+"index-create-time").!
    ("rm -rf "+localDir+"index-space").!
    var i = 1
    for(i <- 1 to 3){
      ("scp yongshangwu@server"+i+":/opt/record/"+index+"/index-create-time "+localDir+"ict"+i).!
      (("cat "+localDir+"ict"+i) #>> new File(localDir+"index-create-time")).!
      ("scp yongshangwu@server"+i+":/opt/record/"+index+"/index-space "+localDir+"is"+i).!
      (("cat "+localDir+"is"+i) #>> new File(localDir+"index-space")).!
    }

    i = 1
    ("rm -rf "+localDir+"query"+query+"-index-load-time").!
    ("rm -rf "+localDir+"query"+query+"-skip").!

    for(i <- 1 to 3){
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
      var line: String = null
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