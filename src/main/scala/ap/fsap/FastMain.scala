package ap.fsap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object FastMain {

  var datasetFileName = "/home/luoyl/test/datasets/k5_n50_d2"
  var maxIteration = 40
  var lambda = 0.5
  var preference = "median"
  var labelFile = s"/home/luoyl/test/aplabel_it${maxIteration}_l${lambda}_p$preference"
  var noise = true
  var sparse = false
  var checkConvergence = true
  var convergenceIter = 10

  def main(args: Array[String]): Unit = {
    if (args.length >= 9) {
      this.datasetFileName = args(0)
      this.labelFile = args(1)
      this.maxIteration = args(2).toInt
      this.lambda = args(3).toDouble
      this.preference = args(4)
      this.noise = args(5).toBoolean
      this.sparse = args(6).toBoolean
      this.checkConvergence = args(7).toBoolean
      this.convergenceIter = args(8).toInt
    }

    val conf = new SparkConf()
      .setAppName("FSAP").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val logger = Logger.getLogger("FSAP")
    logger.setLevel(Level.ALL)

    logger.info("Start affinity propagation")
    val startTick = System.currentTimeMillis()
    val pointsRDD = sc.textFile(datasetFileName).zipWithIndex.map { case (line, id) =>
      val values = line.split(" |,").map(_.toDouble)
      (id, Vectors.dense(values))
    }.cache()

    val similaritiesRDD = pointsRDD.cartesian(pointsRDD)
      .map{ case (u, v) => (u._1, v._1, -Vectors.sqdist(u._2, v._2))}
      .filter(s => s._1 != s._2).persist(StorageLevel.MEMORY_AND_DISK)

    val ap = new FastAP()
      .setMaxIteration(this.maxIteration)
      .setLambda(this.lambda)
      .setPreference(this.preference)
      .setNoise(this.noise)
      .setSparse(this.sparse)
      .setCheckConvergence(this.checkConvergence)
      .setConvergenceIter(this.convergenceIter)

    val label = ap.run(similaritiesRDD).collect()
    logger.info(s"end in ${(System.currentTimeMillis() - startTick) / 1000}s.")

    val sortedLabels = label.map(_._2)

    logger.info(s"numClusters = ${sortedLabels.distinct.length}")
    sortedLabels.foreach(println)

    sc.stop()
  }
}
