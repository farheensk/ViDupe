package vidupe.ffmpeg.phash;


import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;

@RunWith(Parameterized.class)
public class PHash2Test {
    final static String IMG_DIR = "/media/farheen/01D26F1D020D3380/sample/similarityTest";
    private ImagePhash pHash2;
    private String imagePath1;
    private String imagePath2;
    private int distance;
    private int phashDistance;

    public PHash2Test(String imagePath1, String imagePath2, int distance, int phashDistance){
        this.imagePath1 = imagePath1;
        this.imagePath2 = imagePath2;
        this.distance = distance;
        this.phashDistance =  phashDistance;
        this.pHash2 = new ImagePhash();

    }

    @Parameterized.Parameters(name = "{index} : {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390001.jpg", 0, 0 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390002.jpg", 31, 31 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390003.jpg", 27, 31 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390004.jpg", 27, 35 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390005.jpg", 29, 30 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390006.jpg", 29, 30 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390007.jpg", 31, 34 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390008.jpg", 27, 25 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390009.jpg", 30, 30},
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390010.jpg", 25, 27 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390011.jpg", 35, 35 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390012.jpg", 30, 39 },
                {IMG_DIR + "/39/390001.jpg", IMG_DIR+"/39/390013.jpg", 26, 29 }
        });
    }

    @Test
    public void compareDistances() throws Exception{
        String directory1 = IMG_DIR + "/39";
        String directory2 = IMG_DIR + "/53-37";
        List<File> filePaths1 = listImageFiles(directory1);
        List<File> filePaths2 = listImageFiles(directory2);
        for(File f1: filePaths1) {
            for(File f2: filePaths2) {
                InputStream inputStream1 = new FileInputStream(f1);
                InputStream inputStream2 = new FileInputStream(f2);
                String hash1 = pHash2.getHash(inputStream1);
                String hash2 = pHash2.getHash(inputStream2);
                assertEquals(hash2.length(), hash1.length());
                int dist = pHash2.distance(hash1 , hash2);
                assertTrue("distance = " + dist + " is less than 21", dist > 21);
            }
        }
    }

    private List<File> listImageFiles(String directory1) {
        final Collection collection = FileUtils.listFiles(new File(directory1), null, true);
        List<File> filePaths = new ArrayList<File>();
        filePaths.addAll(collection);
        return filePaths;
    }

    @Test
    public void testPhash2() throws Exception {
        InputStream inputStream1 = new FileInputStream(new File(imagePath1));
        InputStream inputStream2 = new FileInputStream(new File(imagePath2));
        String hash1 = pHash2.getHash(inputStream1);
        String hash2 = pHash2.getHash(inputStream2);
        assertEquals(hash2.length(), hash1.length());
        int dist = pHash2.distance(hash1 , hash2);
        assertEquals(distance, dist);
        assertEquals(distance, phashDistance);
    }

    @Test
    public void testPhash2Distance() {
        PHash2 pHash2 = new PHash2();
        int distance = pHash2.distance("001010" , "001011");
        assertEquals(1, distance);
        distance = pHash2.distance("001010111" , "001011101");
        assertEquals(2, distance);
        distance = pHash2.distance("001" , "001");
        assertEquals(0, distance);
        distance = pHash2.distance("0" , "1");
        assertEquals(1, distance);
    }


    @Test
    public void testmatrixMultiplication() {
        PHash2 pHash2 = new PHash2();
        int N = 2;
        double[][] A = new double[N][N];
        double[][] B = new double[N][N];
        for(int i = 0; i<N; i++){
            for(int j=0;j<N; j++){
                A[i][j] =3;
                B[i][j] = 1;
            }
        }
      double result[][] =  pHash2.matrixMultiplication(A,B,N);
       assertEquals(6.0,result[0][0]);
    }

    @Test
    public void testTransposeMatrix() {
        PHash2 pHash2 = new PHash2();
        int N = 4;
        double[][] A = new double[N][N];
        double[][] B = new double[N][N];
        for(int i = 0; i<N; i++){
                A[i][0] =1;
                A[i][1] =2;
                A[i][2] =3;
                A[i][3] =4;
        }
        double[][] result = pHash2.transposeMatrix(A);
        assertEquals(1.0, result[0][3]);
    }

    @Test
    public void testConvolution() throws Exception {
        PHash2 pHash2 = new PHash2();
        BufferedImage image = getBufferedImage();
        float[] kernel = pHash2.getKernel(7, 7, 1.0f);
        BufferedImage convolutedImage = pHash2.getConvolutedImage(image, kernel);
        assertEquals(convolutedImage.getColorModel(), image.getColorModel());
        assertEquals(convolutedImage.getHeight(), image.getHeight());
        assertEquals(convolutedImage.getWidth(), image.getWidth());
        int rgb = convolutedImage.getRGB(0, 0);
        assertEquals(-16777216, rgb);

    }

    private BufferedImage getBufferedImage() throws IOException {
        InputStream fileInputStream = new FileInputStream(new File("/media/farheen/01D26F1D020D3380/sample/similarityTest/39/390001.jpg"));
        return ImageIO.read(fileInputStream);
    }

    @Test
    public void testDCTMatrix() {
        PHash2 pHash2 = new PHash2();
        double[][] dctMatrix = pHash2.phashDCTMatrix(32);
        double[][] expected = new double[][] {};
        compareValues(expected, dctMatrix);
    }

    @Test
    public void medianTest(){
        int size = 5;
        double[] valuesSet = new double[size];
        for(int i=0;i<size;i++){
            valuesSet[i] = i*2;
        }
        PHash2 phash2 = new PHash2();
        double median = phash2.getMedian(valuesSet);
        assertEquals(4.0, median);
    }

//    @Test
//    public void dctMatrixTest(){
//        int N=32;
//        double[][] dctMatrix = pHash2.phashDCTMatrix(N);
//        assertEquals(0.176777, dctMatrix[0][0],0.000001);
//        assertEquals(-0.0122669 , dctMatrix[N-1][N-1],0.000001);
//    }

    private void compareValues(double[][] expected, double[][] dctMatrix) {
        for(int i=0; i< expected.length; i++) {
            for(int j=0;j< expected[i].length; j++ ) {
                assertEquals(expected[i][j], dctMatrix[i][j]);
            }
        }
    }
}