package ian.pdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.junit.Assert._
import java.util.ArrayList
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.JavaConversions._
import scala.collection.mutable
import ian.ISAXIndex._
import java.util.Date

/**
 * @author ${user.name}
 */
object PDD {

  def main(args: Array[String]) {
    val appName = "Parallel Discord Discovery"
    val master = "local[4]"
    val inputfile = "C:/users/ian/github/datasets/testdata1e5.txt"
    val windowSize = 10
    val partitionSize = 4
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val mean = 0.5
    val std = 0.289

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val ts = scala.io.Source.fromFile(inputfile)
      .getLines()
      .toArray
      .map(_.toDouble)
    val bcseqs = sc.broadcast(ts)
    val dh = new DataOfBroadcast(bcseqs, windowSize, mean, std);

    val start = new Date();

    val discords = pdd(sc.parallelize(ts), windowSize, partitionSize, dh, sc)

    val end = new Date();

    for (seq <- discords) {
      println("\t\tBSFDiscord: " + seq.id + "\tdist: " + seq.dist)
    }
    println("Total time elapsed: " + (end.getTime() - start.getTime()) / 1000);
  }

  def pdd(inputData: RDD[Double], winSize: Int, partSize: Int, dh: DataOfBroadcast, sc: SparkContext): Array[Sequence] = {

    val partitioner = new ExactPartitioner(partSize, inputData.count - winSize + 1)
    val bcseqs = sc.broadcast(inputData)

    val ci = new CreateIndex(dh)

    val indices = inputData
      .sliding(winSize)
      .zipWithIndex()
      .map { x => (x._2, x._1) }
      .partitionBy(partitioner)
      .mapPartitions(ci.call, false)

    val batch = 1000

    val chunk = indices.map { _.leafIterator().toArray }
      .flatMap { x => x }
      .map { x => (x.getLoad, x.getIDList.toArray) }
      .reduceByKey(_ ++ _)
      .sortBy(_._2.size)
      .map(x => x._2)
      .flatMap { x => x }
      .map(_.asInstanceOf[Long])
      .zipWithIndex()
      .filter(_._2 < batch)
      .map(_._1.asInstanceOf[java.lang.Long])
      .collect

    var list = sc.parallelize(0 until dh.size().toInt)
      .map { _.toLong }

    val discord = indices.map { x => new NNSearch(dh, chunk).globSearch(x) }
      .flatMap { x => x }
      .map(x => (x.id, x))
      .reduceByKey((x, y) => (if (x.dist < y.dist) x else y))
      .map(x => x._2)
      .reduce((x, y) => (if (x.dist > y.dist) x else y))

    println("Estimated Seqs: " + chunk.size + "\tID: " + discord.id + "\tNeighbor: " + discord.neighbor + "\tDist: " + discord.dist)

    var gBSFDiscord: Sequence = discord
    println("\t\tBSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)

    var q = sc.parallelize(new Array[NNSearch](partSize))
      .map { x => new NNSearch(dh, new Array[java.lang.Long](0), gBSFDiscord.dist, partSize, batch) }
      .zipWithIndex()
      .map(x => (x._2, x._1))

    var numSeqsInQ = q.map(_._2.numSeqs())
      .reduce(_ + _)
    var numSeqsInList = list.count

    while (numSeqsInQ + numSeqsInList > 0) {

      val qIdle = q.filter { !_._2.isBusy }
        .zipWithIndex()
        .map(x => (x._2, x._1))
      val numIdle = qIdle.count
      if (numIdle > 0 && numSeqsInList > 0) {

        // create chunks for filling up
        val qReloads = list.zipWithIndex()
          .filter(_._2 < numIdle * batch)
          .map { x => (x._2 / batch, x._1) }
          .groupByKey
          .rightOuterJoin(qIdle)
          .filter(x => !x._2._1.isEmpty)
          .map(x => (x._2._2._1, reload(x._2._1.get, x._2._2._2)))

        // fill the empty slot of q with the chunks
        q = q.++(qReloads)
          .reduceByKey((x, y) => (if (x.numSeqs() > y.numSeqs()) x else y))
          .sortBy(_._1)

        list = list.zipWithIndex()
          .filter(_._2 >= numIdle * batch)
          .map { _._1 }
      }

      //      println("Before:\t" + q.map(_._2.numDiscord()).collect().mkString("\t"))

      // detect discord from each chunks of q
      val opandres = indices.zipWithIndex()
        .map(x => (x._2, x._1))
        .rightOuterJoin(q)
        .map(x => discovery(x._1, x._2._1.get, x._2._2))

      //      println("After:\t" + q.map(_._2.numDiscord()).collect().mkString("\t"))

      val tempResult = opandres.map(_._2)
        .filter { _.size > 0 }

      q = opandres.map(_._1)

      if (tempResult.count() > 0) {
        val discord = tempResult
          .flatMap { x => x }
          .reduce((x, y) => (if (x.dist > y.dist) x else y))

        println("ID: " + discord.id + "\tNeighbor: " + discord.neighbor + "\tDist: " + discord.dist)

        // update best so far distance
        if (gBSFDiscord.dist < discord.dist) {
          gBSFDiscord = discord
          q = q.map(x => (gBSFDiscord.dist, x))
            .map(x => updateQWithRange(x._2._1, x._2._2, x._1))
          println("\t\tBSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)
        }
      }

      // rotate q
      q = q.map(x => ((x._1 + 1) % partSize, x._2))
        .sortBy(_._1)

      assertTrue("The size of the queue is " + q.count() + ", which is not the same as partitionSize", q.count() == partSize)

      numSeqsInQ = q.map(_._2.numSeqs())
        .reduce(_ + _)
      numSeqsInList = list.count

      println("numSeqs:\t" + (numSeqsInQ + numSeqsInList) + "\t numRemainings:\t" + q.map(_._2.numSeqs()).collect().mkString("\t"))
    }

    Array[Sequence](gBSFDiscord)

  }

  def discovery(id: Long, index: Index, nns: NNSearch): ((Long, NNSearch), Array[Sequence]) = {
    val result = nns.localSearch(id.toInt, index)
    Pair(Pair(id, nns), result)
  }

  def updateQWithRange(id: Long, nns: NNSearch, range: Double): (Long, NNSearch) = {
    nns.setRange(range)
    Pair(id, nns)
  }

  def reload(iter: Iterable[Long], nns: NNSearch): NNSearch = {
    val along = iter.toArray.map(x => x.asInstanceOf[java.lang.Long])
    nns.reload(along)
    nns
  }
}

class CreateIndex(_dh: DataOfBroadcast) extends java.io.Serializable {
  val df = new ED()
  val dh = _dh
  val index = new Index(8, 4, df)

  def call(seqsIter: Iterator[(Long, Array[Double])]): Iterator[Index] = {

    while (seqsIter.hasNext) {
      val seq = seqsIter.next()
      index.add(dh.getViaIterator(seq._2), seq._1)
    }

    val indexlist = List(index)
    indexlist.iterator
  }
}

