package vidupe.ffmpeg.phash;

import java.awt.image.BufferedImage;

public class MyCImgFilter {
    public static int[][] transposeMatrix(int [][] m, int width, int height){
        int[][] temp = new int[height][width];
        for (int i = 0; i < width; i++)
            for (int j = 0; j < height; j++)
                temp[j][i] = m[i][j];
        return temp;
    }

    private static int getRed(BufferedImage img, int x, int y) {
        int rgb = img.getRGB(x, y);
        return (rgb >> 16) & 0xff;
    }

    public BufferedImage myFilter(BufferedImage img){
        BufferedImage result = null;
        //Get all the red channel values
        int size = img.getWidth() * img.getHeight();
        int red[][] = getChannel(img);
        float kernel[][] = getKernel(7 , 7);
        int mx2 = 3, my2 = 3, mx1 = 3, my1 = 3;
        int mxe = img.getWidth() - mx2;
        int mye = img.getHeight() - my2;
        float intermediateResult[][] = getConvoluteImage(red, kernel, img.getWidth(), img.getHeight());
        result = writeImageData(img , intermediateResult);
        return result;
    }

    private BufferedImage writeImageData(BufferedImage img, float[][] intermediateResult) {
        for(int i=0;i<intermediateResult.length;i++){
            for(int j=0;j<intermediateResult[i].length;j++){
                img.setRGB(i, j ,(int)intermediateResult[i][j]);
            }
        }
        return img;
    }

    private float[][] getIntermediateResults(int[][] img, float[][] kernel, int width, int height) {
        float[][] intermediateResult = new float[width][height];
       // int[][] newPaddedImage = paddingWithZeros(img, height, width);
        for(int y = 0; y<(height); ++y) {
            for (int x = 0; x < (width); ++x) {
                float val = 0;
                for (int ym = -3; ym <= 3; ++ym)
                    for (int xm = -3; xm <= 3; ++xm) {
                        if ((x + xm) < width && (y + ym) < height)
                            if ((x + xm) >= 0 && (y + ym) >= 0 && (ym + 3) >= 0 && (xm + 3) >= 0)
                        val += img[x + xm][y + ym] * kernel[xm + 3][ym + 3];
                    }

                intermediateResult[x][y] = val;

            }

//            for (int x = 0; x<width;) {
//                float val = 0;
//                    for (int ym = -3; ym<=3; ++ym)
//                        for (int xm = -3; xm<=3; ++xm){
//                            if ((x + xm) < width && (y + ym) < height)
//                                if ((x + xm) >= 0 && (y + ym) >= 0 && (ym + 3) >= 0 && (xm + 3) >= 0)
//                                    val+=img[x + xm][y + ym]*kernel[xm+3][3 + ym];
//                        }
//                intermediateResult[x][y] = val;
//                if(y<3 || y>(height-3)){
//                    ++x;
//                }
//                else{
//                    if(x<2 || x>=(width-3))
//                        ++x;
//                    else
//                        x=width-3;
//                    }
//                }
//            }
            //boundary conditions

        }
        return new float[0][0];
    }

    private float[][] getConvoluteImage(int[][] img, float[][] kernel, int width, int height) {
        float[][] intermediateResult = new float[width][height];
       // int[][] newPaddedImage = paddingWithZeros(img, height, width);
       // int[][] transposedMatrix = transposeMatrix(img, width, height);
//        for(int x=0; x<height; x++){
//            for(int y=0;y<width; y++){
//                float val = 0;
//                for(int xm=-3; xm<=3 ;xm++) {
//                    for (int ym = -3; ym <= 3; ym++) {
//                        if ((x + xm) < height && (y + ym) < width)
//                        if ((x + xm) >= 0 && (y + ym) >= 0 && (ym + 3) >= 0 && (xm + 3) >= 0)
//                            val += img[x + xm][y + ym] * kernel[ym + 3][xm + 3];
//                    }
//                }
//                intermediateResult[x][y] = val;
//            }
//        }
        for(int y = 3; y<(height-3); ++y) {
            for (int x = 3; x < (width - 3); ++x) {
                float val = 0;
                for (int ym = -3; ym <= 3; ++ym)
                    for (int xm = -3; xm <= 3; ++xm) {
//                        if ((x + xm) < width && (y + ym) < height)
//                            if ((x + xm) >= 0 && (y + ym) >= 0 && (ym + 3) >= 0 && (xm + 3) >= 0)
                        val += img[x + xm][y + ym] * kernel[xm + 3][ym + 3];
                    }

                intermediateResult[x][y] = val;

            }
        }
        for(int y=0;y<height;++y)
            for (int x = 0; x<width;) {
                float val = 0;
                    for (int ym = -3; ym<=3; ++ym)
                        for (int xm = -3; xm<=3; ++xm){
                            if ((x + xm) < width && (y + ym) < height)
                                if ((x + xm) >= 0 && (y + ym) >= 0 && (ym + 3) >= 0 && (xm + 3) >= 0)
                                    val+=img[x + xm][y + ym]*kernel[xm+3][3 + ym];
                        }
                intermediateResult[x][y] = val;
                if(y<3 || y>(height-3)){
                    ++x;
                }
                else{
                    if(x<2 || x>=(width-3))
                        ++x;
                    else
                        x=width-3;
                    }
                }
            //boundary conditions

        return intermediateResult;
    }

    public int[][] paddingWithZeros(int[][] img,int height, int width) {
        int[][] newIntermediateResult = new int[width+6][height+6];
        for(int i=0;i<(width);i++){
            for(int j=0;j<(height);j++)
                newIntermediateResult[i+3][j+3] = img[i][j];
            }
        return newIntermediateResult;
    }

    public float[][] getKernel(int height, int width) {
        int size = height * width;
        float[][] sharpen = new float[width][height];
        for(int j = 0; j<height; j++) {
            for(int i=0; i<width; i++)
            sharpen[i][j] = 1f;
        }
        return sharpen;
    }

    public int[][] getChannel(BufferedImage img) {
        int  red[][] = new int[img.getWidth()][img.getHeight()];
        int k = 0;
        for(int i=0 ; i<img.getWidth();i++){
            for(int j=0; j<img.getHeight();j++){
                int redOriginal = getRed(img, i, j);
                red[i][j] = redOriginal;
               // int test = img.getRGB(i,j);
                k++;
            }
        }
        return red;
    }

}
