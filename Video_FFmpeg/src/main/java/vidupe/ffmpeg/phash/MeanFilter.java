package vidupe.ffmpeg.phash;

import java.awt.*;
import java.awt.image.BufferedImage;

public class MeanFilter {
    public static final int[][] filter49 = {
                                {1, 1, 1, 1, 1, 1, 1},
                                {1, 1, 1, 1, 1, 1, 1},
                                {1, 1, 1, 1, 1, 1, 1},
                                {1, 1, 1, 1, 1, 1, 1},
                                {1, 1, 1, 1, 1, 1, 1},
                                {1, 1, 1, 1, 1, 1, 1},
                                {1, 1, 1, 1, 1, 1, 1},};
    public static final int[][] filter16 = {{1, 2, 1},
            {2, 4, 2},
            {1, 2, 1}
    };

    public static BufferedImage applyMeanFilter(BufferedImage img){
        //get dimensions
        int kernelSize = 49;
        int maxHeight = img.getHeight();
        int maxWidth = img.getWidth();

        //create 2D Array for new picture
        int pictureFile[][] = new int [maxHeight][maxWidth];
        for( int i = 0; i < maxHeight; i++ ){
            for( int j = 0; j < maxWidth; j++ ){
                pictureFile[i][j] = img.getRGB( j, i );
            }
        }

        int output [][] = new int [maxHeight][maxWidth];

        //Apply Mean Filter
        for (int v=1; v<maxHeight; v++) {
            for (int u=1; u<maxWidth; u++) {
                //compute filter result for position (u,v)

                int sum = 0;
                int sumr = 0;
                int sumg = 0;
                int sumb = 0;
                for (int j=-1; j<=1; j++) {
                    for (int i=-1; i<=1; i++) {
                        if((u+(j)>=0) && (v+(i)>=0) && (u+(j)<maxWidth) && (v+(i)<maxHeight)){
                            int pixel=pictureFile[u+i][v+j];
                            int rr=(pixel&0x00ff0000)>>16, rg=(pixel&0x0000ff00)>>8, rb=pixel&0x000000ff;
                            sumr+=rr;
                            sumg+=rg;
                            sumb+=rb;
                            sumr/=kernelSize; sumg/=kernelSize; sumb/=kernelSize;
                            sum=0xff000000|(sumr<<16)|(sumg<<8)|sumb;
                            //sum = sum + p;
                        }
                    }
                }

                int q = (int) (sum /kernelSize);
                output[v][u] = q;
            }
        }

        //Turn the 2D array back into an image
        BufferedImage theImage = new BufferedImage(
                maxHeight,
                maxWidth,
                BufferedImage.TYPE_INT_RGB);
        int value;
        for(int y = 1; y<maxHeight; y++){
            for(int x = 1; x<maxWidth; x++){
                value = output[y][x] ;
                theImage.setRGB(y, x, value);
            }
        }

//        File outputfile = new File("task1output3x3.png");
//        ImageIO.write(theImage, "png", outputfile);
        return theImage;
    }

    public static BufferedImage applyMeanFilter2(BufferedImage img){
        //get dimensions
        int kernelSize = 49;
        int maxHeight = img.getHeight();
        int maxWidth = img.getWidth();

        //create 2D Array for new picture
        int pictureFile[][] = new int [maxHeight][maxWidth];
        for( int i = 0; i < maxHeight; i++ ){
            for( int j = 0; j < maxWidth; j++ ){
                pictureFile[i][j] = img.getRGB( j, i );
            }
        }

        int output [][] = new int [maxHeight][maxWidth];

        //Apply Mean Filter
        for (int v=3; v<maxHeight; v++) {
            for (int u=3; u<maxWidth; u++) {
                //compute filter result for position (u,v)

                int sum = 0;
                int sumr = 0;
                int sumg = 0;
                int sumb = 0;
                for (int j=-3; j<=3; j++) {
                    for (int i=-3; i<=3; i++) {
                        if((u+(j)>=0) && (v+(i)>=0) && (u+(j)<maxWidth) && (v+(i)<maxHeight)){
                            int pixel=pictureFile[v+i][u+j];
                            int rr=(pixel&0x00ff0000)>>16, rg=(pixel&0x0000ff00)>>8, rb=pixel&0x000000ff;
                            sumr+=rr;
                            sumg+=rg;
                            sumb+=rb;
                            sumr/=kernelSize; sumg/=kernelSize; sumb/=kernelSize;
                            sum=0xff000000|(sumr<<16)|(sumg<<8)|sumb;
                            //sum = sum + p;
                        }
                    }
                }

                int q = (int) (sum /kernelSize);
                output[v-3][u-3] = q;
            }
        }

        //Turn the 2D array back into an image
        BufferedImage theImage = new BufferedImage(
                maxHeight,
                maxWidth,
                BufferedImage.TYPE_INT_RGB);
        int value;
        for(int y = 1; y<maxHeight; y++){
            for(int x = 1; x<maxWidth; x++){
                value = output[y][x] ;
                theImage.setRGB(y, x, value);
            }
        }

//        File outputfile = new File("task1output3x3.png");
//        ImageIO.write(theImage, "png", outputfile);
        return theImage;
    }

    public BufferedImage getFilteredImage(BufferedImage givenImage, int iterationNum) {

        int count = 0;
        while (count < iterationNum) {

            for (int y = 3; y + 3 < givenImage.getHeight(); y++) {
                for (int x = 3; x + 3 < givenImage.getWidth(); x++) {
                    Color tempColor = getFilteredValue(givenImage, y, x, filter49);
                    givenImage.setRGB(x, y, tempColor.getRGB());

                }
            }
            count++;
        }
        return givenImage;
    }

    private Color getFilteredValue(final BufferedImage givenImage, int y, int x, int[][] filter) {
        int r = 0, g = 0, b = 0;
        for (int j = -3; j <= 3; j++) {
            for (int k = -3; k <= 3; k++) {

                r += (filter[1 + j][1 + k] * (new Color(givenImage.getRGB(x + k, y + j))).getRed());
                g += (filter[1 + j][1 + k] * (new Color(givenImage.getRGB(x + k, y + j))).getGreen());
                b += (filter[1 + j][1 + k] * (new Color(givenImage.getRGB(x + k, y + j))).getBlue());
            }

        }
        r = r / sum(filter);
        g = g / sum(filter);
        b = b / sum(filter);
        return new Color(r, g, b);
    }

    private int sum(int[][] filter) {
        int sum = 0;
        for (int y = 0; y < filter.length; y++) {
            for (int x = 0; x < filter[y].length; x++) {
                sum += filter[y][x];
            }
        }
        return sum;
    }
}
