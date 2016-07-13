package org.pulasthi.sparkmatrixmultiply.computeonly

import java.nio.{ByteOrder}
import java.util.concurrent.TimeUnit
import com.google.common.base.Stopwatch
import org.apache.commons.cli._
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.pulasthi.sparkmatrixmultiply.{MMUtils, MatrixUtils}
import org.pulasthi.sparkmatrixmultiply.configurations.section.DAMDSSection
import org.pulasthi.sparkmatrixmultiply.damds._


/**
  * Created by pulasthi on 6/19/16.
  */
object Driver {

  var config: DAMDSSection = null;
  var byteOrder: ByteOrder = null;
  var BlockSize: Int = 64;
  var programOptions: Options = new Options();
  var missingDistCount: Accumulator[Int] = null;
  var targetDimension: Int = 3;
  var iterationTimer: Stopwatch = Stopwatch.createUnstarted()
  var iterationTime: Long = 0L;
  var mainTimer: Stopwatch = Stopwatch.createUnstarted()

  def main(args: Array[String]): Unit = {

    mainTimer.start();
    val conf = new SparkConf().setAppName("sparkMDS")
    val sc = new SparkContext(conf)

    val iterations = args(0).toInt
    val globalColCount = args(1).toInt
    ParallelOps.nodeCount = args(2).toInt
    val blockSize = if ((args.length > 3)) args(3).toInt else 64
    ParallelOps.threadCount = if ((args.length > 4)) args(4).toInt else 1

    val parallism = ParallelOps.nodeCount*ParallelOps.threadCount;
    val taskRowCounts: Array[Int] = Array.ofDim[Int](parallism);
    calculaterowCounts(globalColCount,taskRowCounts,parallism);

    //create RDD to make parallel calls
    val runRDD = sc.parallelize(1 to parallism, parallism);
    val rowcount = runRDD.mapPartitionsWithIndex(runTask(globalColCount,targetDimension,taskRowCounts, blockSize, iterations)).count()
    mainTimer.stop();

    println("Total Row Count " + rowcount);
    println("Total Time " + mainTimer.elapsed(TimeUnit.MILLISECONDS));
    println("Iteration Time " + iterationTime);

  }

  def calculaterowCounts(globalRowCount: Int, taskRowCounts: Array[Int], parallism: Int) = {
    val initSize = Math.floor(globalRowCount/parallism).toInt;
    for(i <- 0 until taskRowCounts.length){
      taskRowCounts(i) = initSize;
    }
    val remain = globalRowCount - initSize*parallism;
    for(i <- 0 until remain){
      taskRowCounts(i) += 1;
    }

  }

  def runTask(globalColCount: Int, targetDimension: Int, taskRowCounts: Array[Int], blockSize: Int, iterations: Int)(index: Int, iter: Iterator[Int]) : Iterator[Int] = {
    val localRowCount = taskRowCounts(index);
    val preX: Array[Double] = Array.ofDim[Double](globalColCount * targetDimension);
    val partialBofZ: Array[Array[Double]] = Array.ofDim[Double](localRowCount,globalColCount);;
    val multiplyResult: Array[Double] = Array.ofDim[Double](partialBofZ.length*targetDimension);

    MMUtils.generatePreX(globalColCount,targetDimension,preX);
    MMUtils.generateBofZ(localRowCount,globalColCount,partialBofZ);

    for(i <- 0 until iterations){
      iterationTimer.start();
      MatrixUtils.matrixMultiply(partialBofZ, preX, partialBofZ.length, targetDimension, globalColCount, blockSize, multiplyResult);
      iterationTimer
    }

    println("Task Index " + index + " Number of Rows " + localRowCount)
    return List(localRowCount).iterator;
  }
}
