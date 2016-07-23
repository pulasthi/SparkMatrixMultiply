package org.pulasthi.sparkmatrixmultiply.computeonly

import java.nio.{ByteOrder}
import java.util.concurrent.TimeUnit
import com.google.common.base.Stopwatch
import org.apache.commons.cli._
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.pulasthi.sparkmatrixmultiply.{MMUtils, MatrixUtils}
import org.pulasthi.sparkmatrixmultiply.configurations.section.DAMDSSection

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
  var parallism: Int = 1;


  def main(args: Array[String]): Unit = {

    mainTimer.start();
    val conf = new SparkConf().setAppName("Spark Matrix Multiplication")
    val sc = new SparkContext(conf)

    val iterations = args(0).toInt
    val globalColCount = args(1).toInt
    targetDimension = args(2).toInt
    val blockSize = args(3).toInt
    var rowcount: Int = args(4).toInt
    parallism = args(5).toInt;

    val taskRowCounts: Array[Int] = Array.ofDim[Int](parallism);
    calculaterowCounts(rowcount * parallism,taskRowCounts,parallism);

    //create RDD to make parallel calls
    val runRDD = sc.parallelize(1 to parallism, parallism);
    //With matrix results
    for(i <- 0 until iterations){
      val multiplyResult = runRDD.mapPartitionsWithIndex(runTaskCollect(globalColCount,targetDimension,taskRowCounts, blockSize, iterations)).reduce(collectResults)
    }
    mainTimer.stop();

    println("Total Time for col with matrix reduction :" + globalColCount + " row " + rowcount + " block size " + blockSize + " : " + mainTimer.elapsed(TimeUnit.MILLISECONDS));
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

    //for(i <- 0 until iterations){
      MatrixUtils.matrixMultiply(partialBofZ, preX, partialBofZ.length, targetDimension, globalColCount, blockSize, multiplyResult);
    //}

    println("Task Index " + index + " Number of Rows " + localRowCount)
    return List(localRowCount).iterator;
  }

  def runTaskCollect(globalColCount: Int, targetDimension: Int, taskRowCounts: Array[Int], blockSize: Int, iterations: Int)(index: Int, iter: Iterator[Int]) : Iterator[Array[Double]] = {
    val localRowCount = taskRowCounts(index);
    val preX: Array[Double] = Array.ofDim[Double](globalColCount * targetDimension);
    val partialBofZ: Array[Array[Double]] = Array.ofDim[Double](localRowCount,globalColCount);;
    val multiplyResult: Array[Double] = Array.ofDim[Double](partialBofZ.length*targetDimension);

    MMUtils.generatePreX(globalColCount,targetDimension,preX);
    MMUtils.generateBofZ(localRowCount,globalColCount,partialBofZ);

    //for(i <- 0 until iterations){
    MatrixUtils.matrixMultiply(partialBofZ, preX, partialBofZ.length, targetDimension, globalColCount, blockSize, multiplyResult);
    //}

    println("Task Index " + index + " Number of Rows " + localRowCount)
    return List(multiplyResult).iterator;
  }

  def collectResults(main: Array[Double],next: Array[Double]) : Array[Double] = {
    return main ++ next;
  }

}
