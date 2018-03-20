package vidupe.ffmpeg.phash;

import org.junit.Test;

import java.math.BigInteger;

import static junit.framework.TestCase.assertTrue;

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

    private static long parseLong(String s, int base) {
        return new BigInteger(s, base).longValue();
    }

    public static long makeLong(String input) {
        if(input.substring(0,1).equals("1")) {
            return -1 * (Long.MAX_VALUE - Long.parseLong(input.substring(1), 2) + 1);
        } else {
            return Long.parseLong(input, 2);
        }
    }

    @Test
    public void convertToDouble(){
        String hash = "1000011011100101110100011100000011100110100100011100110101100000";
        String hash1 ="1110100110011000001010000010011001100100000000100010001101001100";
        long longHash = parseLong(hash1, 2);
        System.out.println(longHash);
        //long hash1Long = 9223372036854775807L;
        long hash1Long = -1614496321060592820L;
        String binaryString = Long.toBinaryString(hash1Long);


        String zeros = "0000000000000000000000000000000000000000000000000000000000000000"; //String of 64 zeros
        binaryString = zeros.substring(binaryString.length())+ binaryString;
        int binaryStringLength = binaryString.length();
        int hash1Length = hash1.length();
       assertTrue("lengths not equal",hash1Length == binaryStringLength);
       assertTrue("Strings not equal",binaryString.equals(hash1));
    }
}