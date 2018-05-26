package vidupe.phashgen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/*

 * pHash-like image hash.

 * Author: Elliot Shepherd (elliot@jarofworms.com

 * Based On: http://www.hackerfactor.com/blog/index.php?/archives/432-Looks-Like-It.html

 */
public class ImagePhash {

	private int size = 32;

	private int smallerSize = 8;
	private ColorConvertOp colorConvert = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null);
	private double[] c;
	private static final Logger logger = LoggerFactory.getLogger(ImagePhash.class);

	public ImagePhash() {

		initCoefficients();

	}

// Returns a 'binary string' (like. 001010111011100010) which is easy to do a hamming distance on.

	public ImagePhash(int size, int smallerSize) {

		this.size = size;

		this.smallerSize = smallerSize;

		initCoefficients();

	}

	private static int getBlue(BufferedImage img, int x, int y) {
		return (img.getRGB(x, y)) & 0xff;
	}

	public static void main(String args[]) {

		ImagePhash p = new ImagePhash();
		String image1;
		String image2;
		try {
		    for(int i=0;i<9;i++){
                //image1 = p.getHash(new FileInputStream(new File("D:/optimizilla/crop-min.jpg")));
                image1 = p.getHash(new FileInputStream(new File("/media/farheen/01D26F1D020D3380/sample/thumb720000"+(i+1)+".jpg")));

                image2 = p.getHash(new FileInputStream(new File("/media/farheen/01D26F1D020D3380/sample/thumb240000"+(i+1)+".jpg")));
                logger.info(image1+" "+image2);
                logger.info("1:2 Score is " + p.distance(image1, image2));

            }
        } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int distance(String s1, String s2) {

		int counter = 0;

		for (int k = 0; k < s1.length();k++) {

			if(s1.charAt(k) != s2.charAt(k)) {

				counter++;

			}

		}

		return counter;

	}

	public String getHash(InputStream is) throws Exception {

		BufferedImage img = ImageIO.read(is);

		/* 1. Reduce size.

		 * Like Average Hash, pHash starts with a small image.

		 * However, the image is larger than 8x8; 32x32 is a good size.

		 * This is really done to simplify the DCT computation and not

		 * because it is needed to reduce the high frequencies.

		 */

		img = resize(img, size, size);

		/* 2. Reduce color.

		 * The image is reduced to a grayscale just to further simplify

		 * the number of computations.

		 */

		img = grayscale(img);

		double[][] vals = new double[size][size];

		for (int x = 0; x < img.getWidth(); x++) {

			for (int y = 0; y < img.getHeight(); y++) {

				vals[x][y] = getBlue(img, x, y);

			}

		}

		/* 3. Compute the DCT.

		 * The DCT separates the image into a collection of frequencies

		 * and scalars. While JPEG uses an 8x8 DCT, this algorithm uses

		 * a 32x32 DCT.

		 */

		double[][] dctVals = applyDCT(vals);

		//logger.debug("DCT start");

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

				hash += (dctVals[x][y] > avg?"1":"0");

			}
		}

		return hash;
	}

	private BufferedImage resize(BufferedImage image, int width,    int height) {
		BufferedImage resizedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g = resizedImage.createGraphics();
		g.drawImage(image, 0, 0, width, height, null);
		g.dispose();
		return resizedImage;
	}

	private BufferedImage grayscale(BufferedImage img) {
		colorConvert.filter(img, img);
		return img;
	}

	private void initCoefficients() {
		c = new double[size];

		for (int i=1;i<size;i++) {
			c[i]=1;
		}
		c[0]=1/Math.sqrt(2.0);
	}

	private double[][] applyDCT(double[][] f) {
		int N = size;

		double[][] F = new double[N][N];
		for (int u=0;u<N;u++) {
			for (int v=0;v<N;v++) {
				double sum = 0.0;
				for (int i=0;i<N;i++) {
					for (int j=0;j<N;j++) {
						sum+=Math.cos(((2*i+1)/(2.0*N))*u*Math.PI)*Math.cos(((2*j+1)/(2.0*N))*v*Math.PI)*(f[i][j]);
					}
				}
				sum*=((c[u]*c[v])/4.0);
				F[u][v] = sum;
			}
		}
		return F;
	}
}