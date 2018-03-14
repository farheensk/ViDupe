package vidupe.ffmpeg.phash;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class YoutubeVideoPhash {

    public static void main(String args[]) {
        generateHashes1();
    }

    private static void generateHashes1() {
        String path = "/media/farheen/01D26F1D020D3380/sample/test1";
        Runtime runtime = Runtime.getRuntime();
        BufferedReader br = null;
        List<String> videoIds = new ArrayList<>();
        try {
            br = new BufferedReader(new FileReader(path + "/CommercialsDataset"));
            String line = null;
            while ((line = br.readLine()) != null) {
                String subString = line.substring(1, 12);
                videoIds.add(subString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.print(videoIds.get(0));
        HashMap<String, ArrayList<String>> videoHashes = new HashMap<>();
        HashMap<String, ArrayList<String>> videoHashes2 = new HashMap<>();
        HashMap<String, Integer> numberOfKeyFrames = new HashMap<>();
        int i = 0;
        for (String videoId : videoIds) {
            download(videoId, path + "/download");
            ImagePhash imgphash = new ImagePhash();
            PHash2 pHash2 = new PHash2();
            File directory1 = new File(path + "/download");
            File[] f = directory1.listFiles();
            if (f != null && f.length > 0)
                for (File file : f) {
                    String videoName = file.getName();
                    boolean isWebm = videoName.toLowerCase().endsWith(".webm");
                    boolean isM4a = videoName.toLowerCase().endsWith(".m4a");
                    if (!isM4a && !isWebm) {
                        ArrayList<String> videoHash1 = new ArrayList<>();
                        ArrayList<String> videoHash2 = new ArrayList<>();
                        videoHash1.add(videoId);
                        videoHash2.add(videoId);
                        String fileName = file.getName();
                        String command = "ffmpeg -i " + path + "/download/" + fileName + " -vf select=eq(pict_type\\,PICT_TYPE_I) -vsync vfr " + path + "/keys/video" + i + "-%04d.jpg -hide_banner -y";
                        try {
                            Process p = runtime.exec(command);
                            p.waitFor();
                        } catch (InterruptedException | IOException e) {
                            e.printStackTrace();
                        }
                        String keyFramesPath = path + "/keys";
                        File keysDirectory = new File(keyFramesPath);
                        File[] keyFrames = keysDirectory.listFiles();
                        int keyFrameNumber = 0;
                        for (File keyFrame : keyFrames) {
                            String keyFrameName = keyFrame.getName();
                            boolean isImageFile = keyFrameName.toLowerCase().endsWith(".jpg");
                            boolean matchesF1 = Pattern.matches("video" + i + ".*", keyFrameName);
                            if (isImageFile && matchesF1) {
                                addToList(imgphash, videoHash1, keyFrame);
                                addToPhash2List(pHash2, videoHash2, keyFrame);
                                keyFrameNumber += 1;
                            }
                        }
                        System.out.println("Done videoID" + videoId + keyFrameNumber);
                        numberOfKeyFrames.put("video" + i, keyFrameNumber);
                        videoHashes.put("video" + i, videoHash1);
                        videoHashes2.put("video" + i, videoHash2);
                        i++;
                    }
                    delete(path + "/download/" + videoName);
                }
        }
        writeHashes(videoHashes, path);
        writeHashes(videoHashes2, path+"/phash2");
        for (Map.Entry<String, ArrayList<String>> en : videoHashes.entrySet()) {
            System.out.println(en.getValue());
        }
    }

    private static void writeHashes(HashMap<String, ArrayList<String>> videoHashes, String path) {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(path + "/videoHashesNew1.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(videoHashes);
            out.close();
            fileOut.close();
            System.out.printf("Serialized data is saved in videoHashes.ser");
        } catch (IOException i) {
            i.printStackTrace();
        }
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(path + "/videoHashesNew1.txt"));
            for (Map.Entry<String, ArrayList<String>> en : videoHashes.entrySet()) {

                writer.write(en.getKey());
                writer.write(String.valueOf(en.getValue()));
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void delete(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
    }

    private static void download(String videoId, String path) {
        String command = "youtube-dl --all-formats http://www.youtube.com/watch?v=" + videoId + " -o " + path + "/%(id)s.%(ext)s";
        Runtime runtime = Runtime.getRuntime();
        Process p = null;
//        YoutubeDLRequest request = new YoutubeDLRequest(videoUrl, directory);
//
//        request.setOption("no-mark-watched");
//        request.setOption("ignore-errors");
//        request.setOption("no-playlist");
//        request.setOption("extract-audio");
//        request.setOption("audio-format \"mp3\"");
//        request.setOption("output \"a.%(ext)s\"");
        try {
            p = runtime.exec(command);
            p.waitFor();
            printErrorStream(p);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void addToList(ImagePhash imgphash, ArrayList<String> videoHashList, File file) {
        try {
            videoHashList.add(imgphash.getHash(new FileInputStream(file)));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void addToPhash2List(PHash2 imgphash, ArrayList<String> videoHashList, File file) {
        try {
            videoHashList.add(imgphash.getHash(new FileInputStream(file)));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void printErrorStream(Process p) throws IOException {
        InputStream is = p.getErrorStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        // And print each line
        String s = null;
        while ((s = reader.readLine()) != null) {
            System.out.println(s);
        }
        is.close();
    }

    private static double computeHammingDistance2(ArrayList<String> videoHash1, ArrayList<String> videoHash2) {
        ImagePhash imagePhash = new ImagePhash();
        int counter = 0;
        int N1 = videoHash1.size();
        int N2 = videoHash2.size();
        int den = (N1 <= N2) ? N1 : N2;
        int C[][] = new int[N1 + 1][N2 + 1];

        for (int i = 0; i < N1 + 1; i++) {
            C[i][0] = 0;
        }
        for (int j = 0; j < N2 + 1; j++) {
            C[0][j] = 0;
        }

        for (int i = 1; i < N1 + 1; i++) {
            String videopHash1 = videoHash1.get(i - 1);
            for (int j = 1; j < N2 + 1; j++) {
                String videopHash2 = videoHash2.get(j - 1);
                int distance = imagePhash.hammingDistance(videopHash1, videopHash2);
                if (distance <= 21) {
                    System.out.println("[" + (i - 1) + ", " + (j - 1) + "] = " + distance);
                    C[i][j] = C[i - 1][j - 1] + 1;
                } else {
                    C[i][j] = ((C[i - 1][j] >= C[i][j - 1])) ? C[i - 1][j] : C[i][j - 1];
                }
            }
        }
        double result = (double) (C[N1][N2]) / (double) (den);

        return result;
    }

}
