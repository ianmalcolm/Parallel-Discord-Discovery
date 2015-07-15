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
    var CPI = 20
    var BATCH = 1000
    var REPORT_NUM = 1
    var ESTIMATION = 1000
    var conf = new SparkConf().setAppName(appName)

    if (args.length > 0) {
      val options = new Options();
      options.addOption("fil", true, "The file name of the dataset");
      options.addOption("ofs", true, "Set the offset of the dataset, defalut is 0");
      options.addOption("len", true, "Set the length of dataset, -1 for using the entire dataset, default is -1");
      options.addOption("win", true, "The size of sliding window");
      options.addOption("par", true, "The size of partition");
      options.addOption("bat", true, "The size of batch");
      options.addOption("cpi", true, "The number of chunks per index");
      options.addOption("rep", true, "The number of reported discords");
      options.addOption("est", true, "The number of seqs involved in estimation");
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
      if (cmd.hasOption("cpi")) {
        CPI = Integer.parseInt(cmd.getOptionValue("cpi"));
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
      if (cmd.hasOption("est")) {
        ESTIMATION = Integer.parseInt(cmd.getOptionValue("est"));
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
        + " -cpi " + CPI
        + " -est " + ESTIMATION
        + " -rep " + REPORT_NUM
        + (if (cmd.hasOption("mst")) " -mst " + cmd.getOptionValue("mst") else ""));
    }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext(conf)

    var ts = sc.parallelize(Array[Double]())
    if (LENGTH > 0) {
      ts = sc.textFile(FILE)
        .zipWithIndex()
        .filter(x => x._2 >= OFFSET && x._2 < OFFSET + LENGTH)
        .map(x => x._1.toDouble)
    } else {
      ts = sc.textFile(FILE)
        .map(x => x.toDouble)
    }

    val bcseqs = sc.broadcast(ts.collect())
    val dh = new DataOfBroadcast(bcseqs, SLIDING_WINDOW_SIZE)

    val discords = pdd(ts, PARTITION_SIZE, dh, sc, REPORT_NUM, BATCH, ESTIMATION, CPI)

    val end = new Date();

    for (seq <- discords) {
      println("Top Discord: " + seq.id + "\tdist: " + seq.dist)
    }
    println("Total time elapsed: " + (end.getTime() - start.getTime()) / 1000);

  }

  def pdd(inputData: RDD[Double],
          partSize: Int = 2,
          dh: DataOfBroadcast,
          sc: SparkContext,
          reportNum: Int = 1,
          batch: Int = 1000,
          estimation: Int = 1000,
          cpi: Int = 10): Array[Sequence] = {

    var gBSFDiscord: Sequence = new Sequence(0)

    val partitioner = new ExactPartitioner(partSize, inputData.count - dh.windowSize() + 1)

    val indices = inputData
      .sliding(dh.windowSize())
      .zipWithIndex()
      .map { x => (x._2, x._1) }
      .partitionBy(partitioner)
      .mapPartitions(x => createIndex(x, dh))
      .zipWithIndex()
      .map { x => (x._2, x._1) }

    {
      val chunk = indices.map { _._2.leafIterator() }
        .flatMap { x => x }
        .map { x => (x.getLoad, x.getIDList.toArray) }
        .reduceByKey(_ ++ _)
        .sortBy(_._2.size)
        .map(x => x._2)
        .flatMap { x => x }
        .map(_.asInstanceOf[java.lang.Long])
        .take(estimation)

      val discord = indices.map { x => new NNSearch(dh, chunk).globSearch(x._2) }
        .flatMap { x => x }
        .map(x => (x.id, x))
        .reduceByKey((x, y) => (if (x.dist < y.dist) x else y))
        .map(x => x._2)
        .reduce((x, y) => (if (x.dist > y.dist) x else y))

      logger.warn("Estimated Seqs: " + chunk.size + "\tID: " + discord.id + "\tNeighbor: " + discord.neighbor + "\tDist: " + discord.dist)

      gBSFDiscord = discord
    }

    var q = new Array[NNSearch](partSize * cpi)
      .map { x => new NNSearch(dh, new Array[java.lang.Long](0), gBSFDiscord.dist, partSize) }
      .zipWithIndex
      .map(x => (x._2, x._1))

    var list = 0 until dh.size().toInt

    var numSeqsInQ = q.map(x => x._2.numSeqs())
      .reduce(_ + _)

    while (numSeqsInQ + list.size > 0) {

      val qIdle = q.filter(_._2.numSeqs() < batch)

      if (qIdle.size > 0 && list.size > 0) {

        // create chunks for filling up
        val qReloaded = list.take(qIdle.size * batch)
          .zipWithIndex
          .map { x => (x._2 / batch, x._1.toLong) }
          .groupBy(_._1)
          .map(_._2.map(_._2))
          .zip(qIdle)
          .map(x => reload(x._1.toArray, x._2))

        q = q.filterNot(x => qReloaded.map(y => y._1).contains(x._1))
          .++:(qReloaded)
          .sortBy(_._1)

        list = list.drop(qIdle.size * batch)
      }

      //      logger.info("Before:\t" + q.map(_._2.numSeqs()).collect().mkString("\t"))

      val results = sc.parallelize(q)
        .zipWithIndex
        .map { x => (x._2 / cpi, x._1) }
        .leftOuterJoin(indices)
        .map(x => (x._2._1, (x._1, x._2._2.get)))
        .zipWithIndex
        .map(x => (x._2, x._1))
        .partitionBy(new BalancedPartitioner(q.size))
        .map(x => discovery(x._2._1, x._2._2))
        .collect()

      val curDiscords = results.map(_._2)
        .filter { x => x != null }
        .flatMap { x => x }

      q = results.map(_._1)

      if (!curDiscords.isEmpty) {
        val curDiscord = curDiscords
          .reduce((x, y) => (if (x.dist > y.dist) x else y))

        logger.warn("ID: " + curDiscord.id + "\tNeighbor: " + curDiscord.neighbor + "\tDist: " + curDiscord.dist)

        // update best so far distance
        if (gBSFDiscord.dist < curDiscord.dist) {
          gBSFDiscord = curDiscord
          q.foreach(_._2.setRange(gBSFDiscord.dist))
          logger.warn("BSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)
        }
      }

      // rotate q
      q = q.map(x => ((x._1 + cpi) % q.size, x._2))
        .sortBy(_._1)

      //      assert(q.count() == partSize, "The size of the queue is " + q.count() + ", which is not the same as partitionSize")

      numSeqsInQ = q.map(_._2.numSeqs())
        .reduce(_ + _)

      logger.info("numSeqs:\t" + (numSeqsInQ + list.size))
    }

    val numCalls = q.map { x => x._2.dfCnt }.reduce(_ + _)
    logger.warn("Number of calls to distance function: " + numCalls)
    Array[Sequence](gBSFDiscord)
  }

  def discovery(nns: (Int, NNSearch), index: (Long, Index)): ((Int, NNSearch), Array[Sequence]) = {
    val result = nns._2.localSearch(index._1.toInt, index._2)
    Pair(nns, result)
  }

  def updateQWithRange(id: Long, nns: NNSearch, range: Double): (Long, NNSearch) = {
    nns.setRange(range)
    Pair(id, nns)
  }

  def reload(loads: Array[Long], nns: (Int, NNSearch)): (Int, NNSearch) = {
    nns._2.reload(loads.map(_.asInstanceOf[java.lang.Long]))
    nns
  }

  def createIndex(seqsIter: Iterator[(Long, Array[Double])],
                  dh: DataOfBroadcast,
                  card: Int = 8,
                  dim: Int = 4): Iterator[Index] = {
    val index = new Index(card, dim)

    while (seqsIter.hasNext) {
      val seq = seqsIter.next()
      index.add(dh.getViaIterator(seq._2), seq._1)
    }
    val indexlist = List(index)
    indexlist.iterator
  }
}
