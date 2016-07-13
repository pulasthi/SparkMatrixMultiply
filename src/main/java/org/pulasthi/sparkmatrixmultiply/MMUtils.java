package org.pulasthi.sparkmatrixmultiply;

import java.util.stream.IntStream;

public class MMUtils {
    public static void printAndThrowRuntimeException(RuntimeException e) {
        e.printStackTrace(System.out);
        throw e;
    }

    public static void printAndThrowRuntimeException(String message) {
        System.out.println(message);
        throw new RuntimeException(message);
    }

    public static void generateBofZ(int rowCount, int globalColCount, double[][] partialBofZ){
        /* Set numbers for BofZ*/
        IntStream.range(0, rowCount).forEach(
                r -> IntStream.range(0, globalColCount).forEach(c -> {
                    partialBofZ[r][c] =
                            ((r + c) & 1) == 0 ?
                                    (0.9999995 / 1.0000023) :
                                    (1.0000023 / 0.9999995);
                }));
    }

    public static void generatePreX(
            int numPoints, int targetDim, double[] preX) {
        for (int i = 0; i < numPoints; i++) {
            for (int j = 0; j < targetDim; j++) {
                preX[i*targetDim+j] = (((i+j) & 1) == 0)
                        ? (0.9999995 / 1.0000023) :
                        (1.0000023 / 0.9999995);
            }
        }
    }
}