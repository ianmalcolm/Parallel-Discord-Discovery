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
import scala.collection.mutable.MutableList
import ian.ISAXIndex._

/**
 * @author ${user.name}
 */
object PDD {

  val ABANDON = Long.MinValue

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

    val partitioner = new ExactPartitioner(partitionSize, ts.length - windowSize + 1)
    val bcseqs = sc.broadcast(ts)

    val dh = new DataOfBroadcast(bcseqs, windowSize, mean, std);

    val indices = sc.parallelize(ts)
      .sliding(windowSize)
      .zipWithIndex()
      .map { case (k, v) => (v, k) }
      .partitionBy(partitioner)
      .mapPartitions((new CreateIndex(dh)).call, false)

    //    val freqs = indices.map { _.leafIterator().toArray }
    //      .flatMap { x => x }
    //      .map { x => (x.getLoad, x.numChildren()) }
    //      .reduceByKey(_ + _)
    //      .sortBy(_._2)
    //      .map { case (k, v) => k }
    //      .collect

    val freqs = indices.map { _.leafIterator().toArray }
      .flatMap { x => x }
      .map { x => (x.getLoad, x.numChildren()) }
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .collect

    var gBSFDiscord = new Sequence(ABANDON, ABANDON, Double.NegativeInfinity, false)
    var skipList: List[Long] = List()

    var numCnt = 0
    var words = MutableList[String]()
    val batch = 1000

    for (freq <- freqs) {
      words ++= List(freq._1)
      numCnt += freq._2
      if (numCnt >= batch) {

        val occus = indices.map { _.getOccurences(words.toArray) }
          .flatMap { x => x }
          .collect()
          .filter(x => !skipList.contains(x))

        val discord = indices.map { new LocalNNSearch(dh, occus, gBSFDiscord.dist).search }
          .flatMap { x => x }
          .map(x => (x.id, x))
          .reduceByKey((x, y) => (if (x.dist < y.dist) x else y))
          .map(x => x._2)
          .reduce((x, y) => (if (x.dist > y.dist) x else y))

        println("numSeqs " + occus.size + "\tID " + discord.id + "\tneighbor " + discord.neighbor + "\tdist " + discord.dist)

        if (gBSFDiscord.dist < discord.dist) {
          gBSFDiscord = discord
          val newSkips = indices.map { new Filtering(dh, gBSFDiscord.dist).filter }
            .flatMap { x => x }
            .map { x => x.asInstanceOf[scala.Long] }
            .collect
          skipList = skipList ++ newSkips
          println("\t\tnumSkips " + skipList.size + "\tBSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)
        }

        numCnt = 0
        words = MutableList[String]()
      }

    }

    //    for (word <- freqs) {
    //
    //      val occus = indices.map { _.getOccurence(word) }
    //        .filter { x => x != null }
    //        .flatMap { x => x }
    //        .collect()
    //        .filter(x => !skipList.contains(x))
    //
    //      println("Word: " + word + "\tnumOccurences " + occus.size)
    //
    //      if (occus.size > 0) {
    //        val discord = indices.map { new LocalNNSearch(dh, occus).search }
    //          .flatMap { x => x }
    //          .map(x => (x.id, x))
    //          .reduceByKey((x, y) => (if (x.dist < y.dist) x else y))
    //          .map(x => x._2)
    //          .reduce((x, y) => (if (x.dist > y.dist) x else y))
    //
    //        println("\tID " + discord.id + "\tneighbor " + discord.neighbor + "\tdist " + discord.dist)
    //        if (gBSFDiscord.dist < discord.dist) {
    //          gBSFDiscord = discord
    //          val newSkips = indices.map { new Filtering(dh, gBSFDiscord.dist).filter }
    //            .flatMap { x => x }
    //            .map { x => x.asInstanceOf[scala.Long] }
    //            .collect
    //          skipList = skipList ++ newSkips
    //          println("\t\tnumSkips " + skipList.size + "\tBSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)
    //        }
    //      }
    //    }

    //    for (word <- freqs) {
    //
    //      val _occus = indices.map { _.getOccurence(word) }
    //        .filter { x => x != null }
    //      val occus = _occus.flatMap { x => x }
    //        .collect()
    //
    //      println("Word: " + word + "\tnumPartitions " + _occus.count() + "\tnumOccurences " + occus.size)
    //      
    //      for (occu <- occus) {
    //
    //        if (!skipList.contains(occu)) {
    //          val discord = indices.map { new LocalNNSearch(dh, Array(occu)).search }
    //            .flatMap { x => x }
    //            .reduce((x, y) => (if (x.dist < y.dist) x else y))
    //          println("\tID " + discord.id + "\tneighbor " + discord.neighbor + "\tdist " + discord.dist)
    //          if (gBSFDiscord.dist < discord.dist) {
    //            gBSFDiscord = discord
    //            //            val newSkips = indices.map { new Filtering(dh, gBSFDiscord.dist).filter }
    //            //              .flatMap { x => x }
    //            //              .map { x => x.asInstanceOf[scala.Long] }
    //            //              .collect
    //            //            skipList = skipList ++ newSkips
    //            println("\t\tnumSkips " + skipList.size + "\tBSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)
    //          }
    //        }
    //      }
    //
    //    }

  }

}

class CreateIndex(_dh: DataOfBroadcast) extends java.io.Serializable {
  val df = new ED()
  val dh = _dh
  val index = new Index(df)

  def call(seqsIter: Iterator[(Long, Array[Double])]): Iterator[Index] = {

    while (seqsIter.hasNext) {
      val seq = seqsIter.next()
      index.add(dh.getViaIterator(seq._2), seq._1)
    }

    val indexlist = List(index)
    indexlist.iterator
  }
}

