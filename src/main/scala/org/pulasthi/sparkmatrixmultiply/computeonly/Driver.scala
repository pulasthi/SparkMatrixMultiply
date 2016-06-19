package org.pulasthi.sparkmatrixmultiply.computeonly

import java.nio.ByteOrder

import com.google.common.base.Optional
import edu.indiana.soic.spidal.spark.damds.Constants
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.pulasthi.sparkmatrixmultiply.configurations.ConfigurationMgr
import org.pulasthi.sparkmatrixmultiply.configurations.section.DAMDSSection
import org.pulasthi.sparkmatrixmultiply.damds._

/**
  * Created by pulasthi on 6/19/16.
  */
object Driver {

  var config: DAMDSSection = null;
  var byteOrder: ByteOrder = null;
  var BlockSize: Int = 0;
  var programOptions: Options = new Options();

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
}
