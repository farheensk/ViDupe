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

/*

 * pHash-like image hash.

 * Author: Elliot Shepherd (elliot@jarofworms.com

 * Based On: http://www.hackerfactor.com/blog/index.php?/archives/432-Looks-Like-It.html

 */
public class PHash2 {

    private int size = 32;

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

// Returns a 'binary string' (like. 001010111011100010) which is easy to do a hamming distance on.

    private ColorConvertOp colorConvert = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null);
    private double[] c;

    private BufferedImage resize(BufferedImage image, int width, int height) {
        BufferedImage resizedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = resizedImage.createGraphics();
        g.drawImage(image, 0, 0, width, height, null);
        g.dispose();
        return resizedImage;
    }

    private static int getRed(BufferedImage img, int x, int y) {
        int rgb = img.getRGB(x, y);
        return (rgb >> 16) & 0xff;
    }

    private BufferedImage grayscale(BufferedImage img) {
        colorConvert.filter(img, img);
        return img;
    }

    private BufferedImage toYCrCb(BufferedImage img, int width, int height){
        BufferedImage ycb = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        int red = getRed(img, width, height);
        int green = getGreen(img, width, height);
        int blue = getBlue(img, width, height);
        int y = (int)(0.299* red + 0.589* green + 0.114* blue);
        int cb = (int)(128-0.169*red - 0.331*green + 0.500*blue);
        int cr = (int)(128+0.500*red - 0.419*green - 0.081*blue);


        int val = (y<<16) | (cb<<8) | cr;
        ycb.setRGB(0, 0 ,val);
        System.out.println(red);
        return ycb;
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

    public String getHash(InputStream is) throws Exception {

        BufferedImage img = ImageIO.read(is);

        /* 1. Reduce size.

         * Like Average Hash, pHash starts with a small image.

         * However, the image is larger than 8x8; 32x32 is a good size.

         * This is really done to simplify the DCT computation and not

         * because it is needed to reduce the high frequencies.

         */



        /* 2. Reduce color.

         * The image is reduced to a grayscale just to further simplify

         * the number of computations.

         */

        img = grayscale(img);
        img = getConvolutedImage(img);
        img = resize(img, size, size);
        double[][] vals = new double[size][size];

        for (int x = 0; x < img.getWidth(); x++) {

            for (int y = 0; y < img.getHeight(); y++) {
                // L = R * 299/1000 + G * 587/1000 + B * 114/1000

                //vals[x][y] = (getRed(img, x, y) * 299 / 1000) + (getGreen(img, x, y) * 587 / 1000) + (getBlue(img, x, y) * 114 / 1000);

                // vals[x][y] = getRed(img,x,y);
                // vals[x][y] = getGreen(img,x,y);
                vals[x][y] = getBlue(img,x,y);
            }

        }

        /* 3. Compute the DCT.

         * The DCT separates the image into a collection of frequencies

         * and scalars. While JPEG uses an 8x8 DCT, this algorithm uses

         * a 32x32 DCT.

         */

        long start = System.currentTimeMillis();

        double[][] dctVals = applyDCT(vals);

        //System.out.println("DCT "+(System.currentTimeMillis() - start));

        /* 4. Reduce the DCT.

         * This is the magic step. While the DCT is 32x32, just keep the

         * top-left 8x8. Those represent the lowest frequencies in the

         * picture.

         */

        /* 5. Compute the average value.

         * Like the Average Hash, compute the mean DCT value (using only

         * the 8x8 DCT low-frequency values and excluding the first term

         * since the DC coefficient can be significantly different from

         * the other values and will throw off the average).

         */

        double total = 0;

        for (int x = 0; x < smallerSize; x++) {

            for (int y = 0; y < smallerSize; y++) {

                total += dctVals[x][y];

            }

        }

        total -= dctVals[0][0];

        double avg = total / (double) ((smallerSize * smallerSize) - 1);

        /* 6. Further reduce the DCT.

         * This is the magic step. Set the 64 hash bits to 0 or 1

         * depending on whether each of the 64 DCT values is above or

         * below the average value. The result doesn't tell us the

         * actual low frequencies; it just tells us the very-rough

         * relative scale of the frequencies to the mean. The result

         * will not vary as long as the overall structure of the image

         * remains the same; this can survive gamma and color histogram

         * adjustments without a problem.

         */
        String hash = "";

        for (int x = 0; x < smallerSize; x++) {
            for (int y = 0; y < smallerSize; y++) {
                hash += (dctVals[x][y] > avg ? "1" : "0");
            }
        }

        return hash;
    }

    private void initCoefficients() {
        c = new double[size];

        for (int i = 1; i < size; i++) {
            c[i] = 1;
        }
        c[0] = 1 / Math.sqrt(2.0);
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

    private BufferedImage getConvolutedImage(BufferedImage img) throws IOException {
        BufferedImage dstImage = null;
        float[] sharpen = new float[] {
                0.1f, 0.1f, 0.1f,
                0.1f, 0.1f, 0.1f,
                0.1f, 0.1f, 0.1f
        };
        Kernel kernel = new Kernel(3, 3, sharpen);
        ConvolveOp op = new ConvolveOp(kernel);
        dstImage = op.filter(img, null);
        return dstImage;
    }
}