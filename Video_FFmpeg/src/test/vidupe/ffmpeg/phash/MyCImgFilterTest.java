package vidupe.ffmpeg.phash;

import org.junit.Test;

public class MyCImgFilterTest {

    @Test
    public void myFilter() {
    }

    @Test
    public void getChannel() {
    }

    @Test
    public void paddingWithZeros() {
        int size = 7;
        int[][] initialMatrix = new int[size][size];
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                initialMatrix[i][j] = i + j+1;
            }
        }
        MyCImgFilter myCImgFilter = new MyCImgFilter();
        int[][] newMatrix = myCImgFilter.paddingWithZeros(initialMatrix, size, size);
        printArray(size, initialMatrix);
        printArray(size+6, newMatrix);
    }

    private void printArray(int size, int[][] newMatrix) {
        for (int i = 0; i < size; i++){
            for (int j = 0; j < size; j++) {
                System.out.print(newMatrix[i][j]+ "       ");
            }
            System.out.println();
          }
    }

    @Test
    public void getKernel() {
    }

}