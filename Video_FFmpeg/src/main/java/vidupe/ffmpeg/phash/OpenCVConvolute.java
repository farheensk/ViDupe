package vidupe.ffmpeg.phash;


import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;

import static org.opencv.imgcodecs.Imgcodecs.imwrite;

public class OpenCVConvolute {
    public BufferedImage convolute(BufferedImage img)
    {
        Mat destination = null;
        try {
            int kernelSize = 7;
            Mat source = Img2Mat(img);
           // Mat source = imread("grayscale.jpg", CV_LOAD_IMAGE_GRAYSCALE);
            destination = new Mat(source.rows(),source.cols(),source.type());
            Mat kernel = new Mat(kernelSize,kernelSize, CvType.CV_32F){
                {
                    put(0,0,-1);
                    put(0,1,0);
                    put(0,2,1);

                    put(1,0-2);
                    put(1,1,0);
                    put(1,2,2);

                    put(2,0,-1);
                    put(2,1,0);
                    put(2,2,1);
                }
            };
            Imgproc.filter2D(source, destination, -1, kernel);
            imwrite("output.jpg", destination);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
        BufferedImage dest = mat2Img(destination);
        return dest;
    }
    public BufferedImage mat2Img(Mat in)
    {
        BufferedImage out;
        byte[] data = new byte[8 * 8 * (int)in.elemSize()];
        int type;
        in.get(0, 0, data);

        if(in.channels() == 1)
            type = BufferedImage.TYPE_BYTE_GRAY;
        else
            type = BufferedImage.TYPE_3BYTE_BGR;

        out = new BufferedImage(320, 240, type);

        out.getRaster().setDataElements(0, 0, 320, 240, data);
        return out;
    }

    public Mat Img2Mat(BufferedImage img){
        byte[] data = ((DataBufferByte) img.getRaster().getDataBuffer()).getData();
        Mat mat = new Mat(img.getHeight(), img.getWidth(), CvType.CV_8UC3);
        mat.put(0, 0, data);
        return mat;
    }

}