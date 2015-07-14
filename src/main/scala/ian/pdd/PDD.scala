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
import java.text._
import java.math._

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
    var OFFSET = 0
    var LENGTH = -1
    var CPI = 100
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

      if (cmd.hasOption("bat")) {
        BATCH = Integer.parseInt(cmd.getOptionValue("bat"));
      }
      if (cmd.hasOption("cpi")) {
        CPI = Integer.parseInt(cmd.getOptionValue("cpi"));
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

    val discords = pdd(ts, dh, sc, REPORT_NUM, BATCH, ESTIMATION, CPI)

    val end = new Date();

    for (seq <- discords) {
      println("Top Discord: " + seq.id + "\tdist: " + seq.dist)
    }
    println("Total time elapsed: " + (end.getTime() - start.getTime()) / 1000.0);

  }

  def pdd(inputData: RDD[Double],
          dh: DataOfBroadcast,
          sc: SparkContext,
          reportNum: Int,
          batch: Int,
          estimation: Int,
          cpi: Int = 10): Array[Sequence] = {

    val index = new Index(8, 4)
    for (i <- 0 until dh.size().toInt) {
      index.add(dh.get(i), i)
    }
    val bcIndex = sc.broadcast(index)

    var gBSFDiscord = new Sequence(0);
    {
      val chunk = index.leafIterator()
        .toArray
        .map { x => (x.getLoad, x.getIDList.toArray) }
        .sortBy(_._2.size)
        .map(x => x._2)
        .flatMap(x => x)
        .take(estimation)
        .map(_.asInstanceOf[java.lang.Long])

      val estdiscord = new NNSearch(dh, chunk, bcIndex).globSearch()

      logger.warn("Estimated Seqs: " + chunk.size + "\tID: " + estdiscord.id + "\tNeighbor: " + estdiscord.neighbor + "\tDist: " + estdiscord.dist)

      gBSFDiscord = estdiscord
      logger.warn("\t\tBSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)
    }

    var list = 0 until dh.size().toInt

    var numCalls: Double = 0.0

    while (list.size > 0) {

      // create chunks for filling up
      val loads = list.take((cpi * batch).toInt)
        .zipWithIndex
        .map { x => (x._2 / batch, x._1) }
        .groupBy(_._1)
        .mapValues(_.map(_._2.toLong).toArray)
        .map(x => x._2.map(_.asInstanceOf[java.lang.Long]))
        .toArray

      val q = sc.parallelize(loads)
        .map(x => new NNSearch(dh, bcIndex, x, gBSFDiscord.dist))
        .zipWithIndex()
        .map(x => (x._2, x._1))
        .partitionBy(new BalancedPartitioner(loads.size))
        .map(_._2)

      // fill the empty slot of q with the chunks
      list = list.drop((cpi * batch).toInt)

      val opandres = q.map(x => discovery(x))
        .collect

      numCalls += opandres.map(_._1).reduce(_ + _)
      val curResult = opandres.map(_._2)
        .filter { _ != null }

      if (curResult.size > 0) {
        val curDiscord = curResult.reduce((x, y) => (if (x.dist > y.dist) x else y))
        // update best so far distance
        if (gBSFDiscord.dist < curDiscord.dist) {
          gBSFDiscord = curDiscord
          logger.warn("BSFDiscord: " + gBSFDiscord.id + "\tdist: " + gBSFDiscord.dist)
        }
      }

      logger.info("numSeqs:\t" + (list.size))
    }

    val formatter: NumberFormat = new DecimalFormat("0.###E0")
    logger.warn("Number of calls to distance function: " + formatter.format(numCalls))
    Array[Sequence](gBSFDiscord)

  }

  def discovery(nns: NNSearch): (Long, Sequence) = {
    val result = nns.localSearch()
    Pair(nns.dfCnt, result)
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