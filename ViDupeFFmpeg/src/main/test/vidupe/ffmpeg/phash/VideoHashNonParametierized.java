package vidupe.ffmpeg.phash;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

public class VideoHashNonParametierized {
    final static String IMG_DIR = "/media/farheen/01D26F1D020D3380/sample/DissimilarityVideoTest/test1/";
    final static String IMG_DIR2 = "/media/farheen/01D26F1D020D3380/sample/DissimilarityVideoTest/test1//media/farheen/01D26F1D020D3380/CC_WEB_VIDEO/Videos/";

    private static List<File> listFiles(String directory1) {

        final Collection collection = FileUtils.listFiles(new File(directory1), new String[]{"mp4", "3gp"}, true);
        List<File> filePaths = new ArrayList<File>();
        filePaths.addAll(collection);
        return filePaths;
    }

//    @Test
//    public void computeHammingDistance2() {
//        VideoHash videoHash = new VideoHash();
//        ArrayList<String> videoHash1 = new ArrayList<>(Arrays.asList("10011010", "11101111","11110101"));
//        ArrayList<String> videoHash2 = new ArrayList<>(Arrays.asList("10011010", "11101111","11110101"));
//        final double distance = videoHash.distanceBetweenVideos(videoHash1, videoHash2);
//        assertTrue("videos not equal", distance<19);
//    }

    @Test
    public void getAllVideoHashes() {
        VideoHash videoHash = new VideoHash();
        List<File> videoFiles = listFiles(IMG_DIR);
        List<VideoHashesInformation> videoHashesInformationList = videoHash.getAllVideoHashes(videoFiles);
        final VideoHashesInformation video = videoHashesInformationList.get(0);
        assertTrue("video file names are not similar", video.getVideoName().equals("1.mp4"));
        assertTrue("video hashes not generated", !video.getHashes().isEmpty());
    }

    @Test
    public void dissimilarity() {
        VideoHash videoHash = new VideoHash();
        videoHash.dissimilarity();
    }

    @Test
    public void testCompareTo(){
        File file1 = new File("/media/farheen/01D26F1D020D3380/sample/DissimilarityVideoTest/test1/10.3gp");
        File file2 = new File("/media/farheen/01D26F1D020D3380/sample/DissimilarityVideoTest/test1/8.3gp");
        File[] files = new File[]{file1,file2};
        Arrays.sort(files);
    }

    @Test
    public void testBytesToString() throws UnsupportedEncodingException {
        byte[] b1 = new byte[] {97, 98, 99, -4};

        String s1 = Arrays.toString(b1);
        String s2 = new String(b1);

        System.out.println(s1);        // -> "[97, 98, 99]"
        String[] a = s2.split("");
        System.out.println(a.length);
//        byte b = Byte.parseByte(a[3]);
//        System.out.println(b);
//        String givenString = "stackoverflow abc�";
//        System.out.println(givenString );
//
//
//        byte[] byteValue = givenString.getBytes();
//
//        System.out.println(new String(Arrays.toString(byteValue)));
//
//
//        byte[] byteValueAscii= givenString.getBytes("US-ASCII");
//        System.out.println(Arrays.toString(byteValueAscii));

    }

    @Test
    public void intraComparison() {
        VideoHash videoHash = new VideoHash();
        List<String> videoHashList = new ArrayList<>(Arrays.asList("10101", "10110", "10011"));
        final List<List<String>> lists = videoHash.intraComparison(videoHashList, 19);
        assertTrue("hashes not grouped", lists.size()==1);
    }

    @Test
    public void computeDistance() {
        VideoHash videoHash = new VideoHash();
        ArrayList<String> videoHash1 = new ArrayList<>(Arrays.asList("10011010", "11101111","11110101"));
        ArrayList<String> videoHash2 = new ArrayList<>(Arrays.asList("10011011", "11111111","11111101"));
        final int distance = videoHash.computeDistance(videoHash1, videoHash2);
        assertTrue("wrong distance computed", distance==1);
    }

    @Test
    public void distanceBetweenVideos() {

    }

    @Test
    public void extractAudio() {
        VideoHash videoHash = new VideoHash();
        VideoHashesInformation v = VideoHashesInformation.builder().videoName("1_285_Y.flv").build();
        videoHash.extractAudio(new File("1_285_Y.flv"));
    }
}
