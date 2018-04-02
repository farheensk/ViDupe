package vidupe.ffmpeg.phash;

import com.musicg.fingerprint.FingerprintManager;
import com.musicg.fingerprint.FingerprintSimilarity;
import com.musicg.fingerprint.FingerprintSimilarityComputer;
import com.musicg.wave.Wave;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class VideoHash {

   // final String IMG_DIR = "/media/farheen/01D26F1D020D3380/sample/DissimilarityVideoTest/test1/";
  //  final static String IMG_DIR = "/media/farheen/01D26F1D020D3380/CC_WEB_VIDEO/Videos/";
  // final static String IMG_DIR = "/media/farheen/01D26F1D020D3380/sample/DissimilarityVideoTest/test2/";
   //final static String IMG_DIR = "/media/farheen/01D26F1D020D3380/CC_WEB_VIDEO/2/";
   final static String IMG_DIR = "/media/farheen/01D26F1D020D3380/CC_WEB_VIDEO/test2/";

   final static int threshold = 21;

    final static float[] durations = new float[]{30, 41, 61, 61,
            101, 101, 41, 181, 181, 45, 45, 102, 102, 60, 60, 74, 53, 180, 62, 63, 63};
    final static float[] durationOfTwo = new float[]{1565, 1565};
    static Runtime runtime = Runtime.getRuntime();

    private static List<File> listFiles(String directory1, String[] extensions) {

        final Collection collection = FileUtils.listFiles(new File(directory1), extensions, true);
        List<File> filePaths = new ArrayList<>();
        filePaths.addAll(collection);
        filePaths.sort(new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                String fileName1 = FilenameUtils.removeExtension(file1.getName());
                String fileName2 = FilenameUtils.removeExtension(file2.getName());
                int fileNameInt1;
                int fileNameInt2;
                if(file1.getName().endsWith(".mp4") || file2.getName().endsWith(".mp4") ||
                   file1.getName().endsWith(".flv") || file2.getName().endsWith(".flv") ||
                   file1.getName().endsWith(".MP4") || file2.getName().endsWith(".MP4") ||
                   file1.getName().endsWith(".3gp") || file2.getName().endsWith(".3gp") ||
                   file1.getName().endsWith(".avi") || file2.getName().endsWith(".avi") ||
                   file1.getName().endsWith(".mpg") || file2.getName().endsWith(".mpg") ||
                   file1.getName().endsWith(".rm") || file2.getName().endsWith(".rm")   ||
                   file1.getName().endsWith(".wmv") || file2.getName().endsWith(".wmv")){

                    fileNameInt1 = Integer.parseInt(fileName1.substring(2,fileName1.length()-2));
                    fileNameInt2 = Integer.parseInt(fileName2.substring(2,fileName2.length()-2));
//                    fileNameInt1 = Integer.parseInt(fileName1);
//                    fileNameInt2 = Integer.parseInt(fileName2);
                }
                else if ( file1.getName().endsWith(".mpeg") || file2.getName().endsWith(".mpeg")){
                    fileNameInt1 = Integer.parseInt(fileName1.substring(2,fileName1.length()-3));
                    fileNameInt2 = Integer.parseInt(fileName2.substring(2,fileName2.length()-3));
                }
                else{
                    fileNameInt1 = Integer.parseInt(fileName1);
                    fileNameInt2 = Integer.parseInt(fileName2);
                }
                return fileNameInt1 - fileNameInt2;
            }
        });
        return filePaths;
    }

    private static long parseLong(String s, int base) {
        return new BigInteger(s, base).longValue();
    }

    public void dissimilarity() {
        List<File> videoFiles = listFiles(IMG_DIR, new String[]{"mp4","flv", "MP4", "3gp", "avi", "mpg", "rm", "wmv", "mpeg"});
        List<VideoHashesInformation> videoHashesInformationList = getAllVideoHashes(videoFiles);
        videoHashesInformationList.sort(new MapComparator("numberOfKeyFrames"));
        final List<List<VideoHashesInformation>> lists = groupDuplicateVideoFiles(videoHashesInformationList);
        for(int i=0;i<lists.size();i++){
            for (int j=0;j<lists.get(i).size();j++){
                System.out.print(lists.get(i).get(j).videoName+"   , ");
            }
            System.out.println("\n ================ ");
        }
//        for (VideoHashesInformation video1 : videoHashesInformationList) {
//            for (VideoHashesInformation video2 : videoHashesInformationList) {
//                    double distance = distanceBetweenVideos(video1, video2);
//                    System.out.println(video1.getVideoName() + " " + video2.getVideoName() + ":" + distance);
//
//            }
//            videoHashesInformationList.remove(0);
//            i++;
//        }
//        VideoHashesInformation v1 = videoHashesInformationList.get(2);
//            for (VideoHashesInformation video2 : videoHashesInformationList) {
//                    double distance = distanceBetweenVideos(v1, video2);
//                    System.out.println(v1.getVideoName() + " " + video2.getVideoName() + ":" + distance);
//            }
    }

    public float compareAudioHashes(VideoHashesInformation videoHashesInformation, VideoHashesInformation videoHashesInformation1) {
            FingerprintSimilarity fingerprintSimilarity = new FingerprintSimilarityComputer(videoHashesInformation.getAudioHashes(), videoHashesInformation1.getAudioHashes()).getFingerprintsSimilarity();
            return fingerprintSimilarity.getSimilarity();
    }

    public byte[] getAudioHashes(File file) {
        String fileName1 = FilenameUtils.removeExtension(file.getName());
        byte[] firstFingerPrint = new FingerprintManager().extractFingerprint(new Wave(IMG_DIR+ fileName1+".wav"));
        //final List<Byte> bytes = Arrays.asList(org.apache.commons.lang3.ArrayUtils.toObject(firstFingerPrint));
        //file.setAudioHashes(firstFingerPrint);
        return firstFingerPrint;
    }

    public void extractAudio(File file) {
        String fileName1 = FilenameUtils.removeExtension(file.getName());
        String command = "ffmpeg -i " + file+ " -acodec pcm_s16le -ar 44100 -ac 2 " + IMG_DIR + fileName1+".wav";
        try {
            Process p = runtime.exec(command);
            p.waitFor();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private void makeVideoMatchingDurationSets(List<VideoHashesInformation> videoHashesInformationList) {
        List<List<VideoHashesInformation>> pairedVideos = new ArrayList<>();
        int[] flag = new int[videoHashesInformationList.size()];
        for (VideoHashesInformation vinfo : videoHashesInformationList) {

        }
    }

    public List<VideoHashesInformation> getAllVideoHashes(List<File> videoFilesDirectory) {
        List<VideoHashesInformation> videoHashes = new ArrayList<>();
        int i = 0;
        for (File file1 : videoFilesDirectory) {
            List<String> hashes = generateHashes(file1);
            byte[] audioHashes = generateAudioHashes(file1);
            List<List<String>> hashesAfterIntraComparison = intraComparison(hashes, threshold);
            VideoHashesInformation video1 = VideoHashesInformation.builder().videoName(file1.getName())
                    .hashes(hashesAfterIntraComparison).numberOfKeyFrames(hashesAfterIntraComparison.size()).audioHashes(audioHashes).build();
            i++;
            videoHashes.add(video1);
        }
        return videoHashes;
    }

    public byte[] generateAudioHashes(File file1) {
        extractAudio(file1);
        byte[] audioHashes = getAudioHashes(file1);
        return audioHashes;
    }

    private List<List<VideoHashesInformation>> groupDuplicateVideoFiles1(List<VideoHashesInformation> videoHashesFromStore) {
        int size = videoHashesFromStore.size();
        List<List<VideoHashesInformation>> duplicatesList = new ArrayList<>();
        int[] flag = new int[size];
        for(int i = 0; i < size; i++){
            List<VideoHashesInformation> video1Duplicates = new ArrayList<>();
//            if(flag[i] == 0){
//                flag[i] =1;
                VideoHashesInformation video1 = videoHashesFromStore.get(i);
                video1Duplicates.add(video1);
                for(int j = i + 1; j < size; j++){
                    double distance = distanceBetweenVideos(video1, videoHashesFromStore.get(j));
                    System.out.println(video1.getVideoName()+" "+videoHashesFromStore.get(j).getVideoName()+" "+distance);
                    if(distance>=0.4){
                        flag[j] = 1;

                        video1Duplicates.add(videoHashesFromStore.get(j));
                    }
                }
                duplicatesList.add(video1Duplicates);
//            }
        }
        return duplicatesList;
    }

    private List<List<VideoHashesInformation>> groupDuplicateVideoFiles(List<VideoHashesInformation> videoHashesFromStore) {
        int size = videoHashesFromStore.size();
        List<List<VideoHashesInformation>> duplicatesList = new ArrayList<>();
        int[] flag = new int[size];
        long smallNumKeyFrames;
        for(int i = 0; i < size; i++){
            List<VideoHashesInformation> video1Duplicates = new ArrayList<>();
            if(flag[i] == 0){
                //flag[i] =1;
                VideoHashesInformation video1 = videoHashesFromStore.get(i);
                video1Duplicates.add(video1);
                smallNumKeyFrames = video1.getNumberOfKeyFrames();
                for(int j = i + 1; j < size; j++){
                    VideoHashesInformation video2 = videoHashesFromStore.get(j);
                    double distance = distanceBetweenVideos(video1, video2);
                    System.out.println(distance);
                    if(distance>=0.35){
                        float audioSimilarity = compareAudioHashes(video1, video2);
                        System.out.println(video1.getVideoName()+ " "+ video2.getVideoName()+ audioSimilarity);
                        if(audioSimilarity >0.8){
                            //flag[j] = 1;
                            video1Duplicates.add(video2);
                            if(smallNumKeyFrames>video2.getNumberOfKeyFrames()){
                                video1 = videoHashesFromStore.get(j);
                                smallNumKeyFrames = video1.getNumberOfKeyFrames();
                            }
                        }

                    }
                }
                if(video1Duplicates.size()>1)
                    duplicatesList.add(video1Duplicates);
            }
        }
        return duplicatesList;
    }


    public List<String> generateHashes(File file1) {
        extractKeyFrames(file1);
//        extractAudio(file1);
//        getAudioHashes(file1);
        List<String> generatedHashes = new ArrayList<>();
        List<Long> longHashes = new ArrayList<>();
        final String fileName = FilenameUtils.removeExtension(file1.getName());
        List<File> imageFiles = listFiles(IMG_DIR + fileName, new String[]{"jpg", "jpeg"});
        ImagePhash phash = new ImagePhash();
        for (File f1 : imageFiles) {
            try {
                InputStream inputStream1 = new FileInputStream(f1);
                final String hash = phash.getHash(inputStream1);
                generatedHashes.add(hash);
                longHashes.add(parseLong(hash, 2));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //deleteKeyFrames(file1);
//        System.out.println(longHashes.size());
//        System.out.println(longHashes);
        return generatedHashes;
    }

    private void deleteKeyFrames(File file1) {
        String fileNameWithOutExt = FilenameUtils.removeExtension(file1.getName());
        File extractToDirectory = new File(IMG_DIR + fileNameWithOutExt);
        String[]entries = extractToDirectory.list();
        for(String s: entries){
            File currentFile = new File(file1.getPath(),s);
            currentFile.delete();
        }
        extractToDirectory.delete();
    }

    public void extractKeyFrames(File file1) {
        String fileNameWithOutExt = FilenameUtils.removeExtension(file1.getName());
        File extractToDirectory = new File(IMG_DIR + fileNameWithOutExt);
        extractToDirectory.mkdir();
        String command = "ffmpeg -i " + IMG_DIR + file1.getName() + " -vf select=eq(pict_type\\,PICT_TYPE_I) -vsync vfr " + extractToDirectory + "/%04d.jpg -hide_banner -y";
        try {
            Process p = runtime.exec(command);
            p.waitFor();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    public double distanceBetweenVideos(VideoHashesInformation video1, VideoHashesInformation video2) {
        final List<List<String>> hashesList1 = video1.getHashes();
        int N1 = hashesList1.size();
        final List<List<String>> hashesList2 = video2.getHashes();
        int N2 = hashesList2.size();
        int den = (N1 >= N2) ? N1 : N2;
        int C[][] = new int[N1 + 1][N2 + 1];
        for (int i = 1; i < N1 + 1; i++) {
            List<String> list1 = hashesList1.get(i - 1);
            for (int j = 1; j < N2 + 1; j++) {
                List<String> list2 = hashesList2.get(j - 1);
                int distance = computeDistance(list1, list2);
                if (distance <= threshold)
                    C[i][j] = C[i - 1][j - 1] + 1;
                else
                    C[i][j] = ((C[i - 1][j] >= C[i][j - 1])) ? C[i - 1][j] : C[i][j - 1];
            }
        }
        double result = (double) (C[N1][N2]) / (double) (den);
        return result;
    }

    public int computeDistance(List<String> list1, List<String> list2) {
        ImagePhash imagePhash = new ImagePhash();
        int small = 64;
        for (String hash1 : list1) {
            for (String hash2 : list2) {
                int distance = imagePhash.hammingDistance(hash1, hash2);
                if (distance < small)
                    small = distance;
            }
        }
        return small;
    }

    public List<List<String>> intraComparison(List<String> videoHashesList, int threshold) {
        final int size = videoHashesList.size();
        ImagePhash imagePhash = new ImagePhash();
        List<List<String>> groupedHashes = new ArrayList<>();
        int[] flag = new int[size];
        for (int i = 0; i < size; i++) {
            ArrayList<String> list = new ArrayList<>();
            if (flag[i] == 0) {
                list.add(videoHashesList.get(i));
                for (int j = i + 1; j < size; j++) {
                    double distance = imagePhash.hammingDistance(videoHashesList.get(i), videoHashesList.get(j));
                    if (distance <= threshold) {
                        flag[j] = 1;
                        list.add(videoHashesList.get(j));
                    } else
                        break;
                }
                groupedHashes.add(list);
            }
        }
        return groupedHashes;
    }
}

