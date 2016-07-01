package org.pulasthi.sparkmatrixmultiply;

public class MatrixUtils {

    public static void matrixMultiplyWithThreadOffset(
        double[] A, double[] Adiag, double[] B,
        int aHeight, int bWidth, int comm, int bz, int threadRowOffset, int rowOffset, double[] C) {

        int aHeightBlocks = aHeight / bz; // size = Height of A
        int aLastBlockHeight = aHeight - (aHeightBlocks * bz);
        if (aLastBlockHeight > 0) {
            aHeightBlocks++;
        }

        int bWidthBlocks = bWidth / bz; // size = Width of B
        int bLastBlockWidth = bWidth - (bWidthBlocks * bz);
        if (bLastBlockWidth > 0) {
            bWidthBlocks++;
        }

        int commnBlocks = comm / bz; // size = Width of A or Height of B
        int commLastBlockWidth = comm - (commnBlocks * bz);
        if (commLastBlockWidth > 0) {
            commnBlocks++;
        }

        int aBlockHeight = bz;
        int bBlockWidth = bz;
        int commBlockWidth = bz;

        double kTmp;
        double kiAdiag;
        int iOffset;
        for (int ib = 0; ib < aHeightBlocks; ib++) {
            if (aLastBlockHeight > 0 && ib == (aHeightBlocks - 1)) {
                aBlockHeight = aLastBlockHeight;
            }
            bBlockWidth = bz;
            commBlockWidth = bz;
            for (int jb = 0; jb < bWidthBlocks; jb++) {
                if (bLastBlockWidth > 0 && jb == (bWidthBlocks - 1)) {
                    bBlockWidth = bLastBlockWidth;
                }
                commBlockWidth = bz;
                for (int kb = 0; kb < commnBlocks; kb++) {
                    if (commLastBlockWidth > 0 && kb == (commnBlocks - 1)) {
                        commBlockWidth = commLastBlockWidth;
                    }

                    for (int i = ib * bz; i < (ib * bz) + aBlockHeight; i++) {
                        iOffset=i*bWidth;
                        for (int j = jb * bz; j < (jb * bz) + bBlockWidth;
                             j++) {
                            for (int k = kb * bz;
                                 k < (kb * bz) + commBlockWidth; k++) {
                                kiAdiag = 0;
                                if (i + rowOffset == k) {
                                    kiAdiag = Adiag[i];
                                }
                                else {
                                    //reverse the value from weight
                                    kiAdiag = -(A[(i+threadRowOffset)*comm+k]);
                                }

                                kTmp = B[k*bWidth+j];
                                if (kiAdiag != 0 && kTmp != 0) {
                                    C[iOffset+j] += kiAdiag * kTmp;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static void matrixMultiply(
        double[][] A, double[][] B, int aHeight, int bWidth, int comm, int bz, double[][] C) {

        int aHeightBlocks = aHeight / bz; // size = Height of A
        int aLastBlockHeight = aHeight - (aHeightBlocks * bz);
        if (aLastBlockHeight > 0) {
            aHeightBlocks++;
        }

        int bWidthBlocks = bWidth / bz; // size = Width of B
        int bLastBlockWidth = bWidth - (bWidthBlocks * bz);
        if (bLastBlockWidth > 0) {
            bWidthBlocks++;
        }

        int commnBlocks = comm / bz; // size = Width of A or Height of B
        int commLastBlockWidth = comm - (commnBlocks * bz);
        if (commLastBlockWidth > 0) {
            commnBlocks++;
        }

        int aBlockHeight = bz;
        int bBlockWidth = bz;
        int commBlockWidth = bz;

        for (int ib = 0; ib < aHeightBlocks; ib++) {
            if (aLastBlockHeight > 0 && ib == (aHeightBlocks - 1)) {
                aBlockHeight = aLastBlockHeight;
            }
            bBlockWidth = bz;
            commBlockWidth = bz;
            for (int jb = 0; jb < bWidthBlocks; jb++) {
                if (bLastBlockWidth > 0 && jb == (bWidthBlocks - 1)) {
                    bBlockWidth = bLastBlockWidth;
                }
                commBlockWidth = bz;
                for (int kb = 0; kb < commnBlocks; kb++) {
                    if (commLastBlockWidth > 0 && kb == (commnBlocks - 1)) {
                        commBlockWidth = commLastBlockWidth;
                    }

                    for (int i = ib * bz; i < (ib * bz) + aBlockHeight; i++) {
                        for (int j = jb * bz; j < (jb * bz) + bBlockWidth;
                             j++) {
                            for (int k = kb * bz;
                                 k < (kb * bz) + commBlockWidth; k++) {
                                if (A[i][k] != 0 && B[k][j] != 0) {
                                    C[i][j] += A[i][k] * B[k][j];
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static void matrixMultiply(
        double[][] A, double[] B, int aHeight, int bWidth, int comm, int bz, double[] C) {

        int aHeightBlocks = aHeight / bz; // size = Height of A
        int aLastBlockHeight = aHeight - (aHeightBlocks * bz);
        if (aLastBlockHeight > 0) {
            aHeightBlocks++;
        }

        int bWidthBlocks = bWidth / bz; // size = Width of B
        int bLastBlockWidth = bWidth - (bWidthBlocks * bz);
        if (bLastBlockWidth > 0) {
            bWidthBlocks++;
        }

        int commnBlocks = comm / bz; // size = Width of A or Height of B
        int commLastBlockWidth = comm - (commnBlocks * bz);
        if (commLastBlockWidth > 0) {
            commnBlocks++;
        }

        int aBlockHeight = bz;
        int bBlockWidth = bz;
        int commBlockWidth = bz;

        double kTmp;
        int iOffset;
        for (int ib = 0; ib < aHeightBlocks; ib++) {
            if (aLastBlockHeight > 0 && ib == (aHeightBlocks - 1)) {
                aBlockHeight = aLastBlockHeight;
            }
            bBlockWidth = bz;
            commBlockWidth = bz;
            for (int jb = 0; jb < bWidthBlocks; jb++) {
                if (bLastBlockWidth > 0 && jb == (bWidthBlocks - 1)) {
                    bBlockWidth = bLastBlockWidth;
                }
                commBlockWidth = bz;
                for (int kb = 0; kb < commnBlocks; kb++) {
                    if (commLastBlockWidth > 0 && kb == (commnBlocks - 1)) {
                        commBlockWidth = commLastBlockWidth;
                    }

                    for (int i = ib * bz; i < (ib * bz) + aBlockHeight; i++) {
                        iOffset = i*bWidth;
                        for (int j = jb * bz; j < (jb * bz) + bBlockWidth;
                             j++) {
                            for (int k = kb * bz;
                                 k < (kb * bz) + commBlockWidth; k++) {
                                kTmp = B[k*bWidth+j];
                                if (A[i][k] != 0 && kTmp != 0) {
                                    C[iOffset+j] += A[i][k] * kTmp;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
