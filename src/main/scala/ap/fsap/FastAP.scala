package ap.fsap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Edge, Graph, TripletFields, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


case class EdgeAttribute(s: Double, r: Double, a: Double)

case class EdgeThreshold(s: Double, rh: Double, al: Double, ah: Double)

case class VerticeAttribute(r: Double, a: Double)

class FastAP(
            var maxIterations: Int = 10,
            var damping: Double = 0.5,
            var preference: String = "median",
            var noise: Boolean = true,
            var sparse: Boolean = false,
            var checkConvergence: Boolean = true,
            var convergenceIter: Int = 5
            ) extends Serializable{

  val epsilon: Double = 2.220446049250313e-16
  var _preference: Double = 0.0
  @transient lazy val logger: Logger = Logger.getLogger("FSAP")
  logger.setLevel(Level.ALL)

  def this() {
    this(maxIterations = 10, damping = 0.5)
  }

  def setMaxIteration(maxIteration_ : Int): this.type = {
    this.maxIterations = maxIteration_
    this
  }

  def setLambda(lambda_ : Double): this.type = {
    this.damping = lambda_
    this
  }

  def setPreference(preference_ : String): this.type = {
    this.preference = preference_
    this
  }

  def setNoise(noise_ : Boolean): this.type = {
    this.noise = noise_
    this
  }

  def setSparse(sparse_ : Boolean): this.type = {
    this.sparse = sparse_
    this
  }

  def setCheckConvergence(checkConvergence_ : Boolean): this.type = {
    this.checkConvergence = checkConvergence_
    this
  }

  def setConvergenceIter(iter_ : Int): this.type = {
    this.convergenceIter = iter_
    this
  }

  // TODO: optimize computing median
  def findMedian(similarities: RDD[(Long, Long, Double)]): Double  = {
    logger.info("finding median...")

    val sorted = similarities.sortBy(_._3).zipWithIndex()
      .map{case (triplet, idx) => (idx, triplet)}
      .persist(StorageLevel.MEMORY_AND_DISK)

    val cnt = sorted.count()
    val median = if (cnt % 2 == 0) {
      val rmid = cnt / 2
      val lmid = rmid - 1
      (sorted.lookup(lmid).head._3 + sorted.lookup(rmid).head._3) / 2.0
    }
    else {
      val mid = cnt / 2
      sorted.lookup(mid).head._3
    }
    sorted.unpersist()
    median
  }

  def findMinimum(similarities: RDD[(Long, Long, Double)]): Double  = {
    logger.info("finding minimum...")

    similarities.min()(new Ordering[(Long, Long, Double)]() {
      override def compare(x: (Long, Long, Double), y: (Long, Long, Double)): Int =
        Ordering[Double].compare(x._3, y._3)
    })._3
  }

  def constructGraph(similarities: RDD[(Long, Long ,Double)]): Graph[Double, EdgeAttribute] = {
    val edgeRDD = similarities.map(e => Edge(e._1, e._2, EdgeAttribute(e._3, 0.0, 0.0)))
    Graph.fromEdges(edgeRDD, this._preference)
  }

  def constructSparseGraph(similarities: RDD[(Long, Long ,Double)]): Graph[Double, EdgeAttribute] = {
    // Build graph
    val edgeRDD = similarities.map(e => Edge(e._1, e._2, EdgeAttribute(e._3, 0.0, 0.0)))
    val graphRDD = Graph.fromEdges(edgeRDD, this._preference)

    // Get availability lower bound
    val alV = graphRDD.aggregateMessages[Double](
      sendMsg = ctx => {
        if (ctx.srcId != ctx.dstId)
          ctx.sendToSrc(ctx.srcAttr - ctx.attr.s)
      },
      mergeMsg = math.min,
      TripletFields.Src
    )
    val alG = GraphImpl(alV, graphRDD.edges).mapTriplets(
      et => {
        if (et.srcId != et.dstId)
          EdgeAttribute(et.attr.s, et.attr.r, math.min(0.0, et.srcAttr))
        else
          EdgeAttribute(et.attr.s, et.attr.r, 0.0)
      },
      TripletFields.Src
    )

    // Get Responsibility upper bound
    val rhV = alG.aggregateMessages[Seq[Double]](
      sendMsg = ctx => {
        if (ctx.srcId != ctx.dstId)
          ctx.sendToSrc(Array(ctx.attr.a + ctx.attr.s))
        else
          ctx.sendToSrc(Array(ctx.attr.s))
      },
      mergeMsg = {
        case (seq1, seq2) => (seq1 ++ seq2).sorted.reverse.take(2)
      },
      TripletFields.EdgeOnly
    )
    val rhG = GraphImpl(rhV, alG.edges).mapTriplets(
      et => {
        val seq = et.srcAttr
        val selfValue = et.attr.s + et.attr.a
        val maxValue = if (seq.head == selfValue) seq(1) else seq.head
        EdgeAttribute(et.attr.s, et.attr.s - maxValue, et.attr.a)
      },
      TripletFields.Src
    )

    // Get availability upper bound
    val ahV = rhG.aggregateMessages[Double](
      sendMsg = ctx => {
        if (ctx.srcId != ctx.dstId)
          ctx.sendToDst(math.max(0.0, ctx.attr.r))
        else
          ctx.sendToDst(ctx.attr.r)
      },
      mergeMsg = _ + _,
      TripletFields.EdgeOnly
    )
    val ahG = GraphImpl(ahV, rhG.edges).mapTriplets(
      et => {
        if (et.srcId != et.dstId) {
          val newA = et.dstAttr - math.max(0.0, et.attr.r)
          EdgeThreshold(et.attr.s, et.attr.r, et.attr.a, math.min(0.0, newA))
        }
        else {
          val newA = et.dstAttr - et.attr.r
          EdgeThreshold(et.attr.s, et.attr.r, et.attr.a, newA)
        }
      },
      TripletFields.Dst
    )

    val unfilteredEdges = ahG.edges.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(s"before filter, numEdges=${unfilteredEdges.count()}")

    // Filter edges
    val filteredEdges = unfilteredEdges.filter{
      e => {
        e.attr.rh >= 0 || e.attr.ah >= (-e.attr.rh)
      }
    }.map(e => Edge(e.srcId, e.dstId, EdgeAttribute(e.attr.s, 0.0, 0.0)))
      .persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(s"after filter, numEdges=${filteredEdges.count()}")

    unfilteredEdges.unpersist()

    Graph.fromEdges[Double, EdgeAttribute](filteredEdges, 0.0)
  }

  def apIter(graphRDD: Graph[Double, EdgeAttribute]): Graph[Double, EdgeAttribute] = {
    var iterGraph = graphRDD
    var lastExemplars = new Array[graphx.VertexId](0)
    var nConstant = 0
    var iter = 0
    var converged = false

    while (iter < maxIterations && !converged) {
      logger.info(s"Iteration $iter.")

      // Update responsibilities
      val vD_r = iterGraph.aggregateMessages[Seq[Double]](
        sendMsg = ctx => ctx.sendToSrc(Array(ctx.attr.s + ctx.attr.a)),
        mergeMsg = {
          case (seq1, seq2) => (seq1 ++ seq2).sorted.reverse.take(2)
        },
        TripletFields.EdgeOnly
      )
      // FIXME: seq may be empty
      val updated_r = GraphImpl(vD_r, iterGraph.edges).mapTriplets(
        et => {
          val seq = et.srcAttr
          val selfValue = et.attr.s + et.attr.a
          val maxValue = if (seq.head == selfValue) seq(1) else seq.head
          EdgeAttribute(et.attr.s, damping * et.attr.r + (1 - damping) * (et.attr.s - maxValue), et.attr.a)
        },
        TripletFields.Src
      )

      // Update availabilities
      val vD_a = updated_r.aggregateMessages[Double](
        sendMsg = ctx => {
          if (ctx.srcId != ctx.dstId)
            ctx.sendToDst(math.max(0.0, ctx.attr.r))
          else
            ctx.sendToDst(ctx.attr.r)
        },
        mergeMsg = _ + _,
        TripletFields.EdgeOnly
      )
      iterGraph = GraphImpl(vD_a, updated_r.edges).mapTriplets(
        et => {
          if (et.srcId != et.dstId) {
            val newA = damping * et.attr.a + (1 - damping) * math.min(0.0, et.dstAttr - math.max(0.0, et.attr.r))
            EdgeAttribute(et.attr.s, et.attr.r, newA)
          }
          else
            EdgeAttribute(et.attr.s, et.attr.r, (1 - damping) * (et.dstAttr - et.attr.r) + damping * et.attr.a)
        }
      )

      // Check for convergence
      if (this.checkConvergence) {
        val currExemplars = iterGraph.edges.filter(e => e.srcId == e.dstId && (e.attr.a + e.attr.r) > 0)
          .map(_.srcId).collect()
        if (currExemplars.length == lastExemplars.length
          && currExemplars.diff(lastExemplars).length == 0) {
          nConstant += 1
          converged = nConstant >= convergenceIter
        }
        else
          nConstant = 0
        lastExemplars = currExemplars
        logger.info(s"Number of Exemplars: ${lastExemplars.length}.")
      }
      iter += 1
    }

    if (this.checkConvergence) {
      if (converged)
        logger.info(s"converged after $iter iterations")
      else
        logger.info(s"not converged after $iter iterations with constant times $nConstant")
    }

    iterGraph
  }

  /** Choose exemplars with max{r+a}
  * */
  def chooseExemplar(graph: Graph[Double, EdgeAttribute]): RDD[(VertexId, Int, VertexId)] = {
    val vD = graph.aggregateMessages[(Long, Double)](
      sendMsg = ctx => {
        ctx.sendToSrc((ctx.dstId, ctx.attr.a + ctx.attr.r))
      },
      mergeMsg = {
        case (e1, e2) =>
          if (e1._2 >= e2._2) e1
          else e2
      },
      TripletFields.EdgeOnly
    ).sortBy(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val labelsMap = vD.map(x => x._2._1).distinct().collect().sorted
        .zipWithIndex.toMap

    val sc = vD.sparkContext
    val bcLabelsMap = sc.broadcast(labelsMap)

    vD.map(v => (v._1, bcLabelsMap.value(v._2._1) , v._2._1))
  }

  /** points with (A(i,i) + R(i,i)) > 0 are exemplars.
    * every point choose points k as its exemplar which has
    * maximum similarity from exemplars.
    * return (i, label, center)
  * */
  def chooseExemplar2(graph: Graph[Double, EdgeAttribute]): RDD[(VertexId, Int, VertexId)] = {
    val exemplars = graph.edges.filter(e => e.srcId == e.dstId && (e.attr.a + e.attr.r) > 0)
      .map(_.srcId).collect().sorted
    if (exemplars.length == 0) {
      return chooseExemplar(graph)
    }

    val exemplarsMap = exemplars.zipWithIndex.toMap
    val exemplarsSet = exemplars.toSet

    val sc = graph.vertices.sparkContext

    // Broadcast exemplars set for reducing network communication cost
    val bcExemplarsSet = sc.broadcast(exemplarsSet)
    val bcExemplarsMap = sc.broadcast(exemplarsMap)

    // FIXME: If no neighbor in exemplars ?  Make self a cluster ?
    // If set contains itself, choose self
    graph.aggregateMessages[(VertexId, Double)](
      sendMsg = ctx => {
        if (bcExemplarsSet.value.contains(ctx.srcId))
          ctx.sendToSrc((ctx.srcId, Double.MaxValue))
        else if (bcExemplarsSet.value.contains(ctx.dstId))
          ctx.sendToSrc((ctx.dstId, ctx.attr.s))
      },
      mergeMsg = {
        case (e1, e2) => if (e1._2 >= e2._2) e1 else e2
      },
      TripletFields.EdgeOnly
    ).map(x => (x._1, x._2._1))
      .map(x => (x._1, bcExemplarsMap.value(x._2), x._2))
      .sortBy(_._1)
  }


  def run(similarities: RDD[(Long, Long, Double)]): RDD[(Long, Int, Long)] = {
    // Set preferences
    if (this.preference == "median")
      this._preference = findMedian(similarities)
    else if (this.preference == "minimum")
      this._preference = findMinimum(similarities)
    else
      this._preference = this.preference.toDouble
    logger.info(s"setting preferences = ${this._preference}")
    val preferences = similarities.flatMap(t => Seq(t._1, t._2)).distinct().map(i => (i, i, this._preference))
    val embeddedSimilarities = similarities.union(preferences)

    // Remove degeneracies
    // TODO: gen normal random
    val noisedSimilarities =
      if (!this.noise) embeddedSimilarities
      else{
        logger.info("noising...")
        embeddedSimilarities.map{
          case (uid, vid, s) =>
            (uid, vid, s + (s * this.epsilon + Double.MinPositiveValue * 100) * (math.random - 0.5))
        }
      }

    // Construct graph
    val g =
      if (this.sparse) {
        logger.info("constructing sparse graph...")
        constructSparseGraph(noisedSimilarities)
      }
      else {
        logger.info("constructing dense graph...")
        constructGraph(noisedSimilarities)
      }

    // Iterative computing
    logger.info("enter iteration")
    val convergedGraph = apIter(g)

    // Identify exemplar
    logger.info("choosing exemplar...")
    val label = chooseExemplar2(convergedGraph)

    label
  }


}
