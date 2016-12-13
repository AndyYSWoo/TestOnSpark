package me.yongshang.cbfm.sparktest


import java.io.{FileWriter, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
  * Created by yongshangwu on 2016/12/8.
  */
object DnsDataGenerator {
  def main(args: Array[String]) {
//    val path = "/data/dns/result"
//
//    val conf = new Configuration()
//    conf.set("fs.defaultFS", "hdfs://server1:9000")
//    val fs = FileSystem.get(conf)
//    val out = fs.append(new Path(path))
//    val pw = new PrintWriter(out)
    val pw = new PrintWriter(new FileWriter("/home/yongshangwu/data/DNSData/result", true))
    val random = scala.util.Random
    for(i <- 1 to 590322852){//590322852
      var line = ""
      for(j <- 0 to 3){
        var randomIP = ""
        for(k <- 0 to 3){
          randomIP += random.nextInt(255)
          if(k != 3){
            randomIP += "."
          }
        }
        line += (randomIP + "\t")
      }
      line += ((random.nextInt(145)+5)+"\t")
      line += ((random.nextInt(120)+30)+"\t")
      line += ((random.nextInt(29)+1)*1000+"\t")
      line += ((random.nextInt(5000)+1)*1000+"\t")
      line += "20161002231400\t"
      line += (random.nextInt(65535)+"\t")
      line += (random.nextInt(65535)+"\t")
      line += "010000\t"
      line += "13\t"
      line += "0\t0\t0\t云南\t吉林\t四川\t电信\t--\t--\n"

      pw.write(line)
      if(i%5903228==0){
        print(i/5903228.0+"%")
        pw.flush()
        println(", flush done")
      }
    }
    pw.flush()
    pw.close()
  }
}
