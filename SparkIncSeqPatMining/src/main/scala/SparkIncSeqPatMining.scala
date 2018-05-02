import java.io._
import java.time.{Duration, Instant}
import java.util.ArrayList

import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.reflect.io.Path

object SparkIncSeqPatMining {
  val sc = new SparkContext(new SparkConf().setAppName("SparkIncSeqPatMining").setMaster("spark://134.208.2.165:7077"))
  Logger.getRootLogger().setLevel(Level.OFF)

  def main(args: Array[String]) {
    val runtimestart = Instant.now()
    var config = Source.fromFile("/home/hduser/SparkIncSeqPatMining/config.txt").getLines()
    val wsize = config.next().split(" ")(1).toInt
    val sliding = config.next().split(" ")(1).toInt
    val threshold = config.next().split(" ")(1).toInt
    var slidingtmp = sliding - wsize
    println("windowSize= " + wsize + " sliding= " + sliding + " threshold= " + threshold)

    var uid = 0
    val file = new File("result.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val unitRDDs = new Array[RDD[(String, Int)]](wsize + sliding) //unitRDD array
    var grdd:RDD[(String, Int)] = sc.emptyRDD
    var datapath = "/home/hduser/SparkIncSeqPatMining/data/unit" + uid + ".txt"
    var outpath = "/home/hduser/SparkIncSeqPatMining/output/out" + uid

    while (Path(datapath).exists) {
      val unitrdd = unitCountRDD(datapath)
      unitRDDs(uid % (wsize + sliding)) = unitrdd
	  unitRDDs(uid % (wsize + sliding)).persist()
      slidingtmp += 1

      if (slidingtmp == sliding) {
	    val windowstart = Instant.now()
        if ( uid == (wsize - 1)) {
          for (a <- 0 to (wsize - 1)) {
		    grdd = grdd.union(unitRDDs(a))
		  }
		  grdd = grdd.reduceByKey(_ + _)
        }
        else {
		  //var updateunion:RDD[(String, Int)] = sc.emptyRDD
		  for (a <- 0 to (sliding - 1)) {
			grdd = grdd.union(unitRDDs((uid - a - wsize) % (wsize + sliding)).mapValues(x => x * (-1))).union(unitRDDs((uid - a) % (wsize + sliding))).reduceByKey(_ + _)
			unitRDDs((uid - a - wsize) % (wsize + sliding)).unpersist()
		  }
		  grdd = grdd.filter(_._2 > 0)
		}
		grdd.cache()
        grdd.filter(_._2 >= threshold).keys.saveAsTextFile(outpath)
		slidingtmp = 0
		val windowend = Instant.now()
        bw.write("window " + uid + " " + Duration.between(windowstart, windowend).toMillis + " ms\n")
      }

      uid += 1
      datapath = "/home/hduser/SparkIncSeqPatMining/data/unit" + uid + ".txt"
      outpath = "/home/hduser/SparkIncSeqPatMining/output/out" + uid
    }
    val runtimeend = Instant.now()
    bw.write("runtime " + Duration.between(runtimestart, runtimeend).toMillis + " ms\n")
    bw.close()
  }

  def unitCountRDD(isPath:String) : RDD[(String, Int)] = {
    val trans = sc.textFile(isPath)
    trans.map(t => t.split(" "))
      .flatMap(t => (1.to(t.length).flatMap(x => t.combinations(x)).map(x => x.mkString(" "))))
      .filter(_.contains(" ")).map((_, 1))
      .reduceByKey(_ + _)
  }

}

