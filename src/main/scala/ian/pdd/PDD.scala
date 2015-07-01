package ian.pdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.junit.Assert._
import java.util.ArrayList
import org.apache.log4j.Logger
import org.apache.log4j.Level

import ian.ISAXIndex._

/**
 * @author ${user.name}
 */
object PDD {

  def main(args: Array[String]) {
    val appName = "Parallel Discord Discovery"
    val master = "local[4]"
    val inputfile = "C:/users/ian/github/datasets/testdata1e5.txt"
    val windowSize = 10
    val partitionSize = 10
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

    var indices = sc.parallelize(ts)
      .sliding(windowSize)
      .zipWithIndex()
      .map { case (k, v) => (v, k) }
      .partitionBy(new ExactPartitioner(partitionSize, ts.length - windowSize + 1))
      .mapPartitions((new CreateIndex(dh)).call, false)
      .repartition(partitionSize)

    indices.persist()

    var gBSFDiscord = new Sequence(Double.NegativeInfinity, false)
    var cnt = 0
    var progress = Long.MaxValue

    do {
      val localDiscord = indices.filter { x => !x.isAbandon() }
        .map { new Detection(dh, gBSFDiscord.dist).detect }
        .map { x => x.asInstanceOf[java.lang.Long] }
        .filter { x => x >= 0 && x < dh.size() }
        .collect()

      //      val tempSeqs = indices.map { new GlobNNSearch(dh, localDiscord).search }
      //        .flatMap { x => x }
      //
      //      for (tempSeq <- tempSeqs.collect()) {
      //        //        println(tempSeq.toString())
      //      }
      //    } while (false)

      val globNNSeq = indices.map { new GlobNNSearch(dh, localDiscord).search }
        .flatMap { x => x }
        .map { x => (x.id, x) }
        .reduceByKey{(x, y) => (if (x.dist < y.dist) x else y)}
        .map { case (k, v) => (v) }

      indices.filter { x => !x.isAbandon() }
        .foreach { new Correction(dh, globNNSeq.collect).correct }

      val curDiscord = globNNSeq.reduce((x, y) => (if (x.dist > y.dist) x else y))

      cnt = cnt + 1

      if (gBSFDiscord.dist < curDiscord.dist) {
        gBSFDiscord = curDiscord
      }

      progress = indices.filter { x => !x.isAbandon() }
        .map { x => x.cntRefineReq(gBSFDiscord.dist) }.reduce(_ + _)
      //      println(progress)
      println(cnt + "\t" + progress + "\t" + indices.filter { x => !x.isAbandon() }.count + "\t" + gBSFDiscord.id + "\t" + gBSFDiscord.neighbor + "\t" + gBSFDiscord.dist)

    } while (progress > 0)

  }

  def IsEndOfDiscovery(_gBSFDist: Double, _seqs: RDD[Index]): Boolean = {
    _seqs.map(x => (x.cntRefineReq(_gBSFDist))).count() == 0
  }

  def readFile(filename: String): Array[Double] = {
    val in = scala.io.Source.fromFile(filename).getLines().toArray.map(x => x.toDouble)
    in
  }
}

class CreateIndex(_dh: DataOfBroadcast) extends java.io.Serializable {
  val df = new ED()
  val dh = _dh
  val index = new Index(df)

  def call(seqsIter: Iterator[(Long, Array[Double])]): Iterator[Index] = {

    //    val i = seqsIter.map { case (k, v) => k }.size
    //    val j = seqsIter.map { case (k, v) => k }.size
    //    println("Sequence range: " + i + "\t" + j)

    while (seqsIter.hasNext) {
      val seq = seqsIter.next()
      index.add(dh.getViaIterator(seq._2), seq._1)
      index.addMemo(seq._1)
    }

    //    { // search local nearest neighbor
    //      val selfExcept: ArrayList[java.lang.Long] = new ArrayList[java.lang.Long]()
    //      val i = 41491
    //      for (
    //        overlap <- i - dh.windowSize() + 1 until i
    //          + dh.windowSize()
    //      ) {
    //        selfExcept.add(overlap);
    //      }
    //      val knn = index.knn(dh.get(i), 1, dh, selfExcept)
    //      System.out.println(i + "\t" + knn.get(0) + "\t"
    //        + index.df.distance(dh.get(i), dh.get(knn.get(0))));
    //    }

    val indexlist = List(index)
    indexlist.iterator
  }
}

