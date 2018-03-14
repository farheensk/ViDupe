package vidupe.ffmpeg.phash;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.awt.image.ConvolveOp;
import java.awt.image.Kernel;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.awt.image.BufferedImage.TYPE_BYTE_GRAY;

/*

 * pHash-like image hash.

 * Author: Elliot Shepherd (elliot@jarofworms.com

 * Based On: http://www.hackerfactor.com/blog/index.php?/archives/432-Looks-Like-It.html

 */
public class PHash2 {

    private int size = 32;

    private final double PI = 3.1415926535897932;
    private int smallerSize = 8;

    public PHash2() {

        initCoefficients();

    }

    public PHash2(int size, int smallerSize) {

        this.size = size;

        this.smallerSize = smallerSize;

        initCoefficients();

    }

    public int distance(String s1, String s2) {

        int counter = 0;

        for (int k = 0; k < s1.length(); k++) {

            if (s1.charAt(k) != s2.charAt(k)) {

                counter++;

            }

        }

        return counter;

    }

// Returns a 'binary string' (like. 001010111011100010) which is easy to do a hamming hammingDistance on.

    private ColorConvertOp colorConvert = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null);
    private double[] c;

    public static double[][] transposeMatrix(double [][] m){
        double[][] temp = new double[m[0].length][m.length];
        for (int i = 0; i < m.length; i++)
            for (int j = 0; j < m[0].length; j++)
                temp[j][i] = m[i][j];
        return temp;
    }

    private static int getRed(BufferedImage img, int x, int y) {
        int rgb = img.getRGB(x, y);
        return (rgb >> 16) & 0xff;
    }

    private BufferedImage grayscale(BufferedImage img) {
        colorConvert.filter(img, img);
        return img;
    }

//    private BufferedImage toYCrCb(BufferedImage img, int width, int height){
//        BufferedImage ycb = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
//        int red = getRed(img, width, height);
//        int green = getGreen(img, width, height);
//        int blue = getBlue(img, width, height);
//        int y = (int)(0.299* red + 0.589* green + 0.114* blue);
//        int cb = (int)(128-0.169*red - 0.331*green + 0.500*blue);
//        int cr = (int)(128+0.500*red - 0.419*green - 0.081*blue);
//
//
//        int val = (y<<16) | (cb<<8) | cr;
//        ycb.setRGB(0, 0 ,val);
//        System.out.println(red);
//        return ycb;
//    }

    private BufferedImage resize(BufferedImage image, int width, int height) {
        BufferedImage resizedImage = new BufferedImage(width, height, image.getType());
        Graphics2D g = resizedImage.createGraphics();
        g.drawImage(image, 0, 0, width, height, null);
        g.dispose();
        return resizedImage;
    }

    private BufferedImage toYCrCb(BufferedImage img){
        Color c;
        int red;
        int green;
        int blue;

        for (int i = 0; i < img.getWidth(); i++) {
            for (int j = 0; j < img.getHeight(); j++) {
                c = new Color(img.getRGB(i, j));
                red = c.getRed();
                green = c.getGreen();
                blue = c.getBlue();
                float Y = (66*red + 129*green + 25*blue + 128)/256 + 16;
                float Cb = (-38*red - 74*green + 112*blue + 128)/256 + 128;
                float Cr = (112*red - 94*green - 18*blue + 128)/256 + 128;
//                int y = (int)(0.299* red + 0.589* green + 0.114* blue);
//                int cb = (int)(128-0.169*red - 0.331*green + 0.500*blue);
//                int cr = (int)(128+0.500*red - 0.419*green - 0.081*blue);
                int resultY = cut(Y, 0, 255);
                int resultCb = cut(Cb, 0, 255);
                int resultCr = cut(Cr, 0, 255);

                int val = (resultY<<16) | (resultCb<<8) | resultCr;
                img.setRGB(i,j,val);
            }
        }
        return img;
    }


    private static int getGreen(BufferedImage img, int x, int y) {
        int rgb = img.getRGB(x, y);
        return (rgb >> 8) & 0xff;
    }

    private static int getBlue(BufferedImage img, int x, int y) {
        return (img.getRGB(x, y)) & 0xff;
    }

    public static void main(String args[]) {
        PHash2 p = new PHash2();
        String image1;
        String image2;
        String path1 = "/media/farheen/01D26F1D020D3380/sample/similarityTest/39/";
        String path2 = "/media/farheen/01D26F1D020D3380/sample/similarityTest/53-37/";
        try {
            image1 = p.getHash(new FileInputStream(new File(path1 + "390001.jpg")));

            image2 = p.getHash(new FileInputStream(new File(path2 + "53-370001.jpg")));
            System.out.println(image1 + " " + image2);
            System.out.println("1:2 Score is " + p.distance(image1, image2));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int cut(float val, int min, int max) {
        if(val<min)
            return min;
        else if(val>max)
            return max;
        return Math.round(val);
    }

    public String getHash(InputStream is) throws Exception {

        BufferedImage img = ImageIO.read(is);
        int channels = img.getColorModel().getNumComponents();
        if(channels == 3) {
           img = toYCrCb(img);
           img = getImageFromOneChannel(img);
           img = getConvolutedImage(img, getKernel(7, 7, 1.0f));
         //   img = MeanFilter.applyMeanFilter2(img);
            MyCImgFilter cImgFilter = new MyCImgFilter();
            img = cImgFilter.myFilter(img);
//            for(int i=0;i<img.getHeight();i++){
//                for(int j=0;j<img.getWidth();j++){
//                    Color c = new Color(img.getRGB(i,j));
//                    int red = c.getRed();
//                    int green = c.getGreen();
//                    int blue = c.getBlue();
//                }
//            }

        } else if(channels == 4){
            throw new RuntimeException("Channels =4");

        } else {
            throw new RuntimeException("Unknown channels");
        }

        img = resize(img, size, size);


        /* 3. Compute the DCT.

         * The DCT separates the image into a collection of frequencies

         * and scalars. While JPEG uses an 8x8 DCT, this algorithm uses

         * a 32x32 DCT.

         */


        double[][] dctImage = performDCT(img);

        double[] subsec = unroll(dctImage);
        double median = getMedian(subsec);

//        double avg = total / (double) ((smallerSize * smallerSize -1));

        StringBuilder hash = new StringBuilder();

        for (int x = 0; x < smallerSize; x++) {
            for (int y = 0; y < smallerSize; y++) {
                hash.append(dctImage[x][y] > median ? "1" : "0");
            }
        }

        return hash.toString();
    }

    double[][] performDCT(BufferedImage img) {
        double[][] intensityValues = getIntensityValues(img);
        double[][] dctVals = phashDCTMatrix(32);
        double[][] dtValsTranspose = transposeMatrix(dctVals);
        double[][] dctImage = matrixMultiplication(dctVals,intensityValues,32);
        dctImage = matrixMultiplication(dctImage,dtValsTranspose, 32);
        return dctImage;
    }

    public double getMedian(double[] dctImage) {
        List<Double> values = new ArrayList<Double>();
        for (int x = 0; x < dctImage.length; x++) {
                values.add(dctImage[x]);
            }
        Collections.sort(values);
        double median = values.get((dctImage.length)/2);
        return median;
    }

    private void initCoefficients() {
        c = new double[size];

        for (int i = 1; i < size; i++) {
            c[i] = 1;
        }
        c[0] = 1 / Math.sqrt(2.0);
    }

    private double[][] getIntensityValues(BufferedImage img) {
        double[][] vals = new double[size][size];

        for (int x = 0; x < size; x++) {

            for (int y = 0; y < size; y++) {
                // L = R * 299/1000 + G * 587/1000 + B * 114/1000

              vals[x][y] = (getRed(img, x, y) * 299 / 1000) + (getGreen(img, x, y) * 587 / 1000) + (getBlue(img, x, y) * 114 / 1000);

                // vals[x][y] = img.getRGB(x,y);
                // vals[x][y] = getGreen(img,x,y);
                // vals[x][y] = getRed(img,x,y);
            }

        }
        return vals;
    }

    private double[][] applyDCT(double[][] f) {
        int N = size;

        double[][] F = new double[N][N];
        for (int u = 0; u < N; u++) {
            for (int v = 0; v < N; v++) {
                double sum = 0.0;
                for (int i = 0; i < N; i++) {
                    for (int j = 0; j < N; j++) {
                        sum += Math.cos(((2 * i + 1) / (2.0 * N)) * u * Math.PI) * Math.cos(((2 * j + 1) / (2.0 * N)) * v * Math.PI) * (f[i][j]);
                    }
                }
                sum *= ((c[u] * c[v]) / 4.0);
                F[u][v] = sum;
            }
        }
        return F;
    }

    double[][] phashDCTMatrix(int N){
        double[][] F = new double[N][N];
        for(int i=0;i<N;i++){
            for(int j=0;j<N; j++){
                F[i][j] = 1/Math.sqrt(N);
            }
        }
        double c1 = Math.sqrt((double)2.0/N);

        for (int x=0;x<N;x++){
            for (int y=1;y<N;y++){
                F[x][y] = c1*Math.cos((PI/2/N)*y*(2*x+1));
            }
        }
        return F;
    }

    BufferedImage getConvolutedImage(BufferedImage img, float[] filter) {
        BufferedImage dstImage;
        int height, width;
        height = width = (int)Math.sqrt(filter.length);
        Kernel kernel = new Kernel(width, height, filter);
        ConvolveOp op = new ConvolveOp(kernel, ConvolveOp.EDGE_ZERO_FILL, null);
        dstImage = op.filter(img, null);
        return dstImage;
    }

    float[] getKernel(int height, int width, float value) {
        int size = height * width;
        float[] sharpen = new float[size];
        for(int i = 0; i< size; i++) {
            sharpen[i] = value/size;
        }
        return sharpen;
    }

    public BufferedImage getGrayScaleFromColor(BufferedImage img) throws IOException {
        Color c;
        Color tempColor;
        int red;
        int green;
        int blue;

        for (int x = 0; x < img.getWidth(); x++) {
            for (int y = 0; y < img.getHeight(); y++) {
                c = new Color(img.getRGB(x, y));
                red = c.getRed();
                //green = c.getGreen();
                green = 0;
               // blue = c.getBlue();
                blue = 0;
                int grayScaleVal = (int) (0.21 * red + 0.72 * green + 0.07 * blue);
                tempColor = new Color(grayScaleVal, grayScaleVal, grayScaleVal);
                img.setRGB(x, y, tempColor.getRGB());
            }
        }
        return img;
    }

    //TODO: Test case
    public BufferedImage getImageFromOneChannel(BufferedImage img) {
        Color c;
        Color tempColor;
        int red;
        for (int x = 0; x < img.getWidth(); x++) {
            for (int y = 0; y < img.getHeight(); y++) {
                c = new Color(img.getRGB(x, y));
                red = c.getRed();
                tempColor = new Color(red, 0, 0);
                img.setRGB(x, y, tempColor.getRGB());
            }
        }
        return img;
    }

    double[] unroll(double[][] img) {
        double[] result = new double[8];
        System.arraycopy(img[1], 1, result, 0, 8);
        return result;
    }

    private BufferedImage resizeToGray(BufferedImage image, int width, int height) {
        BufferedImage resizedImage = new BufferedImage(width, height, TYPE_BYTE_GRAY);
        Graphics2D g = resizedImage.createGraphics();
        g.drawImage(image, 0, 0, width, height, null);
        g.dispose();
        return resizedImage;
    }

    public double[][] matrixMultiplication(double[][] A, double[][] B, int N){
        double[][] C = new double[N][N];
        for (int i = 0; i < N; i++)
        {
            for (int j = 0; j < N; j++)
            {
                for (int k = 0; k < N; k++)
                {
                    C[i][j] = C[i][j] + A[i][k] * B[k][j];
                }
            }
        }
        return C;
    }

}