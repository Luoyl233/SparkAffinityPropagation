package ap.distap

import java.io.PrintWriter

import ap.fsap.FastAP
import breeze.linalg._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD

class DistAP(
            maxIter: Int = 60,
            damping: Double = 0.5,
            preference: Option[Double] = None
            )
  extends Serializable {

  @transient val logger: Logger = Logger.getLogger("DisAP")
  logger.setLevel(Level.ALL)

  def euclideanDistances(X: DenseMatrix[Double]): DenseMatrix[Double] = {
    val XX = sum(X :* X, Axis._1)

    var distances = X * X.t

    distances :*= -2.0
    distances = distances(::, *) + XX
    distances = distances(*, ::) + XX

    distances
  }

  // Brute force solution
  def matrixMedian(X: DenseMatrix[Double]): Double = {
    val rows = X.rows
    val cols = X.cols
    val len = rows * cols

    val sorted = X.toArray.sorted

    if (len % 2 == 0) {
      val rmid = len / 2
      val lmid = rmid - 1
      (sorted(lmid) + sorted(rmid)) / 2
    }
    else {
      val mid = len / 2
      sorted(mid) / 2
    }
  }

  // Local affinity propagation using matrix computation
  def localAP(points: Array[(Long, org.apache.spark.ml.linalg.Vector)],
              damping: Double = 0.5,
              maxIter: Int = 200,
              preference: Option[Double] = None,
              convergenceIter: Int = 15
             ): Array[(Long, org.apache.spark.ml.linalg.Vector)] = {
    val nSamples = points.length
    val ind = DenseVector.range(0, nSamples, 1)

    // Compute similarity matrix
    val S = -euclideanDistances(DenseMatrix(points.map(_._2.toArray): _*))

    // Set preference
    if (preference.isEmpty) {
      val median = matrixMedian(S)
      for (i <- ind)
        S(i, i) = median
    }
    else {
      for (i <- ind)
        S(i, i) = preference.get
    }

    // Initialize messages
    var R = DenseMatrix.zeros[Double](nSamples, nSamples)
    var A = DenseMatrix.zeros[Double](nSamples, nSamples)

    // TODO: Noise

    var preExemplars = new Array[Int](0).toSet
    var converged = false
    var nConstant = 0

    // Update messages
    for (iter <- 0 until maxIter if !converged) {
      // Compute responsibilities
      var tmp = S :+ A
      val I = argmax(tmp, Axis._1)
      val Y = DenseVector({
        for (i <- ind) yield tmp(i, I(i))
      }.toArray)

      for (i <- ind)
        tmp(i, I(i)) = -breeze.numerics.inf
      val Y2 = max(tmp, Axis._1)

      tmp = S(::, *) - Y
      for (i <- ind)
        tmp(i, I(i)) = S(i, I(i)) - Y2(i)

      tmp :*= 1 - damping
      R :*= damping
      R :+= tmp

      // Compute availabilities
      tmp = clip(R, 0.0, breeze.numerics.inf)
      for (i <- ind)
        tmp(i, i) = R(i, i)

      val s = sum(tmp, Axis._0).t
      tmp = tmp(*, ::) - s
      val dA = diag(tmp)
      tmp = clip(tmp, 0.0, breeze.numerics.inf)
      for (i <- ind)
        tmp(i, i) = dA(i)

      tmp :*= 1 - damping
      A :*= damping
      A :-= tmp

      val curExemplars = ((diag(A) + diag(R)) :> 0.0).activeIterator
        .filter(_._2).map(_._1).toSet

      if (curExemplars.size == preExemplars.size
          && curExemplars.diff(preExemplars).isEmpty) {
        nConstant += 1
        converged = nConstant >= convergenceIter
      }
      else {
        nConstant = 0
      }
      preExemplars = curExemplars

      // Inapplicable solution of Python in scala
//      val E = ((diag(A) + diag(R)) :> 0.0).map(x => if(x) 1 else 0)
//      e(::, iter % convergenceIter) := E
//
//      if (iter >= convergenceIter) {
//        val se = sum(e, Axis._1)
//        unconverged = sum((se :== convergenceIter) :+ (se :== 0)) != nSamples
//      }
    }

    // Identify exemplars
    val indices =
      if (preExemplars.nonEmpty) {
        preExemplars.toArray
      }
      // Return argmax{A+R} if no points i have {A+R} > 0
      // Just in case of error
      else {
        argmax(A :+ R, Axis._1).toArray.distinct
      }

    val exemplars = for (e <- indices) yield points(e)
    exemplars
  }

  def globalAP(localExemplars: RDD[(Long, org.apache.spark.ml.linalg.Vector)],
               damping: Double = 0.5,
               maxIter: Int = 200,
               preference: Option[Double] = None,
               convergenceIter: Int = 15
              ): Array[(Long, org.apache.spark.ml.linalg.Vector)] = {
    val nLocalExemplars = localExemplars.count()

    // Compute super exemplars on big dataset using GraphX
    if (nLocalExemplars >= 10000) {
      val similarities = localExemplars.cartesian(localExemplars)
        .map { case (p1, p2) =>
          (p1._1, p2._1, -Vectors.sqdist(p1._2, p2._2))
        }.filter(s => s._1 != s._2)

      val fastap = new FastAP(
        maxIterations = maxIter,
        damping = damping,
        noise = false,
        checkConvergence = false
      )
      val superExemplarsId = fastap.run(similarities).map(_._3).distinct().collect().toSet
      localExemplars.filter(p => superExemplarsId.contains(p._1)).collect()
    }
    else {
      localAP(localExemplars.collect(),
        damping = damping,
        maxIter = maxIter,
        preference = preference
      )
    }
  }

  def chooseExemplars(points: RDD[(Long, org.apache.spark.ml.linalg.Vector)],
                      superExemplars:Array[(Long, org.apache.spark.ml.linalg.Vector)]
                     ): RDD[(Long, Int, Long)] = {
    val sc = points.sparkContext
    val bcse = sc.broadcast(superExemplars)

    points.mapPartitions { pointsInPartition =>
      val candidates = bcse.value
      pointsInPartition.map { p =>
        var minIdx = -1
        var minDist = Double.MaxValue
        for (i <- candidates.indices) {
          val dist = Vectors.sqdist(p._2, candidates(i)._2)
          if (dist < minDist) {
            minDist = dist
            minIdx = i
          }
        }
        (p._1, minIdx,candidates(minIdx)._1)
      }
    }
  }

  def run(points: RDD[(Long, org.apache.spark.ml.linalg.Vector)]): RDD[(Long, Int, Long)] = {
    // Compute local exemplars
    val localExemplars = points.mapPartitions { points =>
      localAP(points.toArray,
        damping = this.damping,
        maxIter = this.maxIter,
        preference = this.preference
      ).iterator
    }.cache()

    // Debug need
//    val out = new PrintWriter("/home/luoyl/test/localexemplars")
//    for (e <- localExemplars.collect())
//      out.println(e._2.toArray.mkString(","))
//    out.close()

//    val nLocalExemplars = localExemplars.count()
//    logger.info(s"Number of local exemplars: $nLocalExemplars")

    // Compute super/global exemplars
    val superExemplars = globalAP(
      localExemplars,
      damping = this.damping,
      maxIter = this.maxIter,
      preference = this.preference
    ).sortBy(_._1)

    logger.info(s"Super exemplars: ${superExemplars.map(_._1).mkString(",")}")

    // Choose super exemplars
    val pointsWithLabel = chooseExemplars(points, superExemplars)

    pointsWithLabel.sortBy(_._1)
  }
}
