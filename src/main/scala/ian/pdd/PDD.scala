package ian.pdd

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.collection.JavaConversions._
import scala.collection.mutable
import ian.ISAXIndex._
import java.util.Date
import java.util.ArrayList

/**
 * @author ${user.name}
 */
object PDD {

  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    val start = new Date();

    val appName = "Parallel Discord Discovery"
    var master = ""
    var FILE = ""
    var SLIDING_WINDOW_SIZE = 100
    var PARTITION_SIZE = 4
    var OFFSET = 0
    var LENGTH = -1
    var BATCH = 1000
    var REPORT_NUM = 1
    var conf = new SparkConf().setAppName(appName)

    if (args.length > 0) {
      val options = new Options();
      options.addOption("fil", true, "The file name of the dataset");
      options.addOption("ofs", true, "Set the offset of the dataset, defalut is 0");
      options.addOption("len", true, "Set the length of dataset, -1 for using the entire dataset, default is -1");
      options.addOption("win", true, "The size of sliding window");
      options.addOption("par", true, "The size of partition");
      options.addOption("bat", true, "The size of batch");
      options.addOption("rep", true, "The number of reported discords");
      options.addOption("mst", true, "The configuration of master");
      options.addOption("log", true, "The log level");
      options.addOption("h", false, "Print help message");

      val parser: CommandLineParser = new BasicParser();
      val cmd: CommandLine = parser.parse(options, args);

      if (cmd.hasOption("h")) {
        val formatter: HelpFormatter = new HelpFormatter();
        formatter.printHelp("HOTSAX", options);
        return ;
      }
      if (cmd.hasOption("fil")) {
        FILE = cmd.getOptionValue("fil");
      }
      if (cmd.hasOption("ofs")) {
        OFFSET = Integer.parseInt(cmd.getOptionValue("ofs"));
      }
      if (cmd.hasOption("len")) {
        LENGTH = Integer.parseInt(cmd.getOptionValue("len"));
      }
      if (cmd.hasOption("win")) {
        SLIDING_WINDOW_SIZE = Integer.parseInt(cmd.getOptionValue("win"));
      }
      if (cmd.hasOption("par")) {
        PARTITION_SIZE = Integer.parseInt(cmd.getOptionValue("par"));
      }
      if (cmd.hasOption("bat")) {
        BATCH = Integer.parseInt(cmd.getOptionValue("bat"));
      }
      if (cmd.hasOption("rep")) {
        REPORT_NUM = Integer.parseInt(cmd.getOptionValue("rep"));
      }
      if (cmd.hasOption("mst")) {
        val MASTER = cmd.getOptionValue("mst");
        conf = conf.setMaster(MASTER)
      }
      if (cmd.hasOption("log")) {
        if (cmd.getOptionValue("log").equalsIgnoreCase("OFF")) {
          LogManager.getRootLogger().setLevel(Level.OFF)
        } else if (cmd.getOptionValue("log").equalsIgnoreCase("FATAL")) {
          LogManager.getRootLogger().setLevel(Level.FATAL)
        } else if (cmd.getOptionValue("log").equalsIgnoreCase("ERROR")) {
          LogManager.getRootLogger().setLevel(Level.ERROR)
        } else if (cmd.getOptionValue("log").equalsIgnoreCase("WARN")) {
          LogManager.getRootLogger().setLevel(Level.WARN)
        } else if (cmd.getOptionValue("log").equalsIgnoreCase("INFO")) {
          LogManager.getRootLogger().setLevel(Level.INFO)
        } else if (cmd.getOptionValue("log").equalsIgnoreCase("DEBUG")) {
          LogManager.getRootLogger().setLevel(Level.DEBUG)
        } else if (cmd.getOptionValue("log").equalsIgnoreCase("TRACE")) {
          LogManager.getRootLogger().setLevel(Level.TRACE)
        } else if (cmd.getOptionValue("log").equalsIgnoreCase("ALL")) {
          LogManager.getRootLogger().setLevel(Level.ALL)
        }
      }

      println("-fil " + FILE
        + " -ofs " + OFFSET
        + " -len " + LENGTH
        + " -win " + SLIDING_WINDOW_SIZE
        + " -par " + PARTITION_SIZE
        + " -bat " + BATCH
        + " -rep " + REPORT_NUM
        + (if (cmd.hasOption("mst")) " -mst " + cmd.getOptionValue("mst") else ""));
    }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext(conf)

    var ts: Array[Double] = Array()
    if (LENGTH > 0) {
      ts = Array.ofDim[Double](LENGTH)
      var i = 0;
      var ofsCnt = 1;
      val iter = scala.io.Source.fromFile(FILE).getLines()
      while (ofsCnt < OFFSET) {
        ofsCnt += 1
        if (iter.hasNext) {
          val line = iter.next()
        } else {
          System.out.println("Not enough lines in " + FILE);
          exit
        }
      }
      while (i < LENGTH) {
        if (iter.hasNext) {
          ts(i) = iter.next().toDouble
        } else {
          System.out.println("Not enough lines in " + FILE);
          exit
        }
        i += 1
      }
    } else {
      ts = scala.io.Source.fromFile(FILE)
        .getLines()
        .toArray
        .map(_.toDouble)
    }

    val bcseqs = sc.broadcast(ts)
    val dh = new DataOfBroadcast(bcseqs, SLIDING_WINDOW_SIZE)

    val discords = pdd(sc.parallelize(ts), SLIDING_WINDOW_SIZE, PARTITION_SIZE, dh, sc, REPORT_NUM, BATCH)

    val end = new Date();

    for (seq <- discords) {
      println("Top Discord: " + seq.id + "\tdist: " + seq.dist)
    }
    println("Total time elapsed: " + (end.getTime() - start.getTime()) / 1000);

  }

  def pdd(inputData: RDD[Double],
          winSize: Int,
          partSize: Int,
          dh: DataOfBroadcast,
          sc: SparkContext,
          reportNum: Int,
          batch: Int): Array[Sequence] = {

    val partitioner = new ExactPartitioner(partSize, inputData.count - winSize + 1)
    val bcseqs = sc.broadcast(inputData)

    val ci = new CreateIndex(dh)

    val indices = inputData
      .sliding(winSize)
      .zipWithIndex()
      .map { x => (x._2, x._1) }
      .partitionBy(partitioner)
      .mapPartitions(ci.call, false)

    val chunk = indices.map { _.leafIterator().toArray }
      .flatMap { x => x }
      .map { x => (x.getLoad, x.getIDList.toArray) }
      .reduceByKey(_ ++ _)
      .sortBy(_._2.size)
      .map(x => x._2)
      .flatMap { x => x }
      .map(_.asInstanceOf[Long])
      .zipWithIndex()
      .filter(_._2 < 1000)
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

    logger.warn("Estimated Seqs: " + chunk.size + "\tID: " + discord.id + "\tNeighbor: " + discord.neighbor + "\tDist: " + discord.dist)

    var gBSFDiscord: Sequence = discord
    logger.warn("\t\tBSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)

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

      logger.info("Before:\t" + q.map(_._2.numSeqs()).collect().mkString("\t"))

      // detect discord from each chunks of q
      val opandres = indices.zipWithIndex()
        .map(x => (x._2, x._1))
        .rightOuterJoin(q)
        .map(x => discovery(x._1, x._2._1.get, x._2._2))

      //      logger.debug("After:\t" + q.map(_._2.numSeqs()).collect().mkString("\t"))

      val tempResult = opandres.map(_._2)
        .filter { _.size > 0 }

      q = opandres.map(_._1)

      if (tempResult.count() > 0) {
        val discord = tempResult
          .flatMap { x => x }
          .reduce((x, y) => (if (x.dist > y.dist) x else y))

        logger.warn("ID: " + discord.id + "\tNeighbor: " + discord.neighbor + "\tDist: " + discord.dist)

        // update best so far distance
        if (gBSFDiscord.dist < discord.dist) {
          gBSFDiscord = discord
          q = q.map(x => (gBSFDiscord.dist, x))
            .map(x => updateQWithRange(x._2._1, x._2._2, x._1))
          logger.warn("BSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)
        }
      }

      // rotate q
      q = q.map(x => ((x._1 + 1) % partSize, x._2))
        .sortBy(_._1)

      assert(q.count() == partSize, "The size of the queue is " + q.count() + ", which is not the same as partitionSize")

      numSeqsInQ = q.map(_._2.numSeqs())
        .reduce(_ + _)
      numSeqsInList = list.count

      logger.info("numSeqs:\t" + (numSeqsInQ + numSeqsInList) + "\t numRemainings:\t" + q.map(_._2.numSeqs()).collect().mkString("\t"))
    }

    val numCalls = indices.map { x => x.df.getCount }.reduce(_ + _)
    logger.warn("Number of calls to distance function: " + numCalls)
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
  val dh = _dh
  val index = new Index(8, 4)

  def call(seqsIter: Iterator[(Long, Array[Double])]): Iterator[Index] = {

    while (seqsIter.hasNext) {
      val seq = seqsIter.next()
      index.add(dh.getViaIterator(seq._2), seq._1)
    }

    val indexlist = List(index)
    indexlist.iterator
  }
}

