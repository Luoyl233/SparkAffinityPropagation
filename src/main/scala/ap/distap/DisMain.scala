package ap.distap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object DisMain {
  var datasetFileName = "file:///home/luoyl/test/datasets/k5_n50_d2"
  var maxIteration = 40
  var damping = 0.5
  var preference: Option[Double] = None
  var K = 3

  def main(args: Array[String]): Unit = {
    if (args.length >= 5) {
      this.datasetFileName = args(0)
      this.maxIteration = args(1).toInt
      this.damping = args(2).toDouble
      this.K = args(4).toInt
    }

    val conf = new SparkConf()
      .setAppName("DisAP").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val logger = Logger.getLogger("DisAP")
    logger.setLevel(Level.ALL)

    val startTick = System.currentTimeMillis()

    val pointsRDD = sc.textFile(this.datasetFileName).zipWithIndex.map{case (line, id) =>
      val values = line.split(" |,").map(_.toDouble)
      (id, Vectors.dense(values))
    }.repartition(this.K)
      .cache()

    val disap = new DistAP(
      maxIter = this.maxIteration,
      damping = this.damping,
      preference = this.preference
    )
    val labels = disap.run(pointsRDD)
    labels.map(x => (x._2, x._3)).collect()//.foreach(x => println(x._1 + "," + x._2))

    logger.info(s"End in ${(System.currentTimeMillis() - startTick) / 1000}S")

    sc.stop()
  }
}
