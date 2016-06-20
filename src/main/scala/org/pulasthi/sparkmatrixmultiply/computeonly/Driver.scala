package org.pulasthi.sparkmatrixmultiply.computeonly

import java.io.IOException
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Random
import java.util.regex.Pattern

import com.google.common.base.{Strings, Optional}
import edu.indiana.soic.spidal.common.{RangePartitioner, Range}
import edu.indiana.soic.spidal.spark.damds.ParallelOps
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.pulasthi.sparkmatrixmultiply.configurations.ConfigurationMgr
import org.pulasthi.sparkmatrixmultiply.configurations.section.DAMDSSection
import org.pulasthi.sparkmatrixmultiply.damds._

import scala.io.Source

/**
  * Created by pulasthi on 6/19/16.
  */
object Driver {

  var config: DAMDSSection = null;
  var byteOrder: ByteOrder = null;
  var BlockSize: Int = 0;
  var programOptions: Options = new Options();
  var palalizem : Int = 8

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("sparkMDS")
    val sc = new SparkContext(conf)

    val parserResult: Optional[CommandLine] = parseCommandLineArguments(args, Driver.programOptions);

    if (!parserResult.isPresent) {
      println(Constants.ErrProgramArgumentsParsingFailed)
      new HelpFormatter().printHelp(Constants.ProgramName, programOptions)
      return
    }

    val cmd: CommandLine = parserResult.get();
    if (!(cmd.hasOption(Constants.CmdOptionLongC) && cmd.hasOption(Constants.CmdOptionLongN) && cmd.hasOption(Constants.CmdOptionLongT))) {
      println(Constants.ErrInvalidProgramArguments)
      new HelpFormatter().printHelp(Constants.ProgramName, Driver.programOptions)
      return
    }

    if (!parserResult.isPresent) {
      println(Constants.ErrProgramArgumentsParsingFailed)
      new HelpFormatter().printHelp(Constants.ProgramName, Driver.programOptions)
      return
    }
    readConfigurations(cmd)

    var hdoopconf = new Configuration();
    var blockpointcount = (math.ceil(config.numberDataPoints.toDouble/palalizem)).toInt
    var blockbtyesize = blockpointcount*config.numberDataPoints*2;
    hdoopconf.set("mapred.min.split.size", ""+blockbtyesize);
    hdoopconf.set("mapred.max.split.size", ""+blockbtyesize);

    var preX: Array[Array[Double]] = if (Strings.isNullOrEmpty(config.initialPointsFile))
      generateInitMapping(config.numberDataPoints, config.targetDimension)
    else readInitMapping(config.initialPointsFile, config.numberDataPoints, config.targetDimension);

    val ranges: Array[Range] = RangePartitioner.Partition(0, config.numberDataPoints, 1)
    ParallelOps.procRowRange = ranges(0);
    var datardd = sc.binaryRecords(config.distanceMatrixFile,2*config.numberDataPoints,hdoopconf);
    datardd.repartition(palalizem)

    val shortsrdd : RDD[Array[Short]] = datardd.map{ cur =>
    {
      val shorts: Array[Short] = Array.ofDim[Short](cur.length/2)
      ByteBuffer.wrap(cur).asShortBuffer().get(shorts)
      shorts
    }}

    //TODO : check if we need to replace 0 with min values


  }

  def readConfigurations(cmd: CommandLine): Unit = {
    Driver.config = ConfigurationMgr.LoadConfiguration(
      cmd.getOptionValue(Constants.CmdOptionLongC)).damdsSection;
    //TODO check if this is always correct
    ParallelOps.globalColCount = config.numberDataPoints;
    ParallelOps.nodeCount =
      Integer.parseInt(cmd.getOptionValue(Constants.CmdOptionLongN));
    ParallelOps.threadCount =
      Integer.parseInt(cmd.getOptionValue(Constants.CmdOptionLongT));
    ParallelOps.mmapsPerNode = if (cmd.hasOption(Constants.CmdOptionShortMMaps)) cmd.getOptionValue(Constants.CmdOptionShortMMaps).toInt else 1;
    ParallelOps.mmapScratchDir = if (cmd.hasOption(Constants.CmdOptionShortMMapScrathDir)) cmd.getOptionValue(Constants.CmdOptionShortMMapScrathDir) else ".";

    Driver.byteOrder =
      if (Driver.config.isBigEndian) ByteOrder.BIG_ENDIAN else ByteOrder.LITTLE_ENDIAN;
    Driver.BlockSize = Driver.config.blockSize;
  }

  def parseCommandLineArguments(args: Array[String], opts: Options): Optional[CommandLine] = {
    val optParser: CommandLineParser = new GnuParser();
    try {
      return Optional.fromNullable(optParser.parse(opts, args))
    }
    catch {
      case e: ParseException => {
        e.printStackTrace
      }
    }
    return Optional.fromNullable(null);
  }


  def generateInitMapping(numPoints: Int, targetDim: Int): Array[Array[Double]] = {
    var x: Array[Array[Double]] = Array.ofDim[Double](numPoints, targetDim);
    val rand: Random = new Random(System.currentTimeMillis)
    for (row <- x) {
      for (i <- 0 until row.length) {
        row(i) = if (rand.nextBoolean) rand.nextDouble else -rand.nextDouble
      }
    }
    return x;
  }

  def readInitMapping(initialPointsFile: String, numPoints: Int, targetDimension: Int): Array[Array[Double]] = {
    try {
      var x: Array[Array[Double]] = Array.ofDim[Double](numPoints, targetDimension);
      var line: String = null
      val pattern: Pattern = Pattern.compile("[\t]")
      var row: Int = 0
      for (line <- Source.fromFile(initialPointsFile).getLines()) {
        if (!Strings.isNullOrEmpty(line)) {
          val splits: Array[String] = pattern.split(line.trim)

          for (i <- 0 until splits.length) {
            x(row)(i) = splits(i).trim.toDouble
          }
          row += 1;
        }
      }
      return x;
    } catch {
      case ex: IOException => throw new RuntimeException(ex)
    }
  }
}
