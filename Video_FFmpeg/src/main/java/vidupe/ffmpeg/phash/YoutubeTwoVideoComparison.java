package vidupe.ffmpeg.phash;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class YoutubeTwoVideoComparison {
   public static void main(String args[]) {
        String directory = "/media/farheen/01D26F1D020D3380/sample/test";
        String command = "ffmpeg -i " + directory + "/240.mp4 -vf select=eq(pict_type\\,PICT_TYPE_I) -vsync vfr " + directory + "/f1%04d.jpg -hide_banner -y";
        String command2 = "ffmpeg -i " + directory + "/file3.3gp -vf select=eq(pict_type\\,PICT_TYPE_I) -vsync vfr " + directory + "/f2%04d.jpg -hide_banner -y";
        Runtime runtime = Runtime.getRuntime();
        BufferedReader br = null;
        List<String> videoIds = new ArrayList<>();
        try {
            br = new BufferedReader(new FileReader(directory + "/videoIdsDurations.txt"));
            String line = null;
            while ((line = br.readLine()) != null) {
                String subString = line.substring(1, 12);
                videoIds.add(subString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String videoId : videoIds) {
            download(videoId, directory);
        }

        try {
            // System.out.println("Cmd:" + command);
            Process p = runtime.exec(command);
            p.waitFor();
            p = runtime.exec(command2);
            p.waitFor();
            printErrorStream(p);

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();

        }
        ImagePhash imgphash = new ImagePhash();
        File directory1 = new File(directory);
        ArrayList<String> videoHash1 = new ArrayList<>();
        ArrayList<String> videoHash2 = new ArrayList<>();
        File[] f = directory1.listFiles();
        if (f != null && f.length > 0)
            for (File file : f) {
                String fileName = file.getName();
                boolean isImageFile = file.getName().toLowerCase().endsWith(".jpg");
                boolean matchesF1 = Pattern.matches("f1.*", fileName);
                boolean matchesF2 = Pattern.matches("f2.*", fileName);
                if (isImageFile && matchesF2) {
                    addToList(imgphash, videoHash1, file);
                } else if (isImageFile && matchesF1) {
                    addToList(imgphash, videoHash2, file);
                }

            }
        double similarity = computeHammingDistance2(videoHash1, videoHash2);
        System.out.println(similarity);
    }

    private static void download(String videoId, String path) {
        String command = "youtube-dl --all-formats http://www.youtube.com/watch?v=" + videoId + " -o " + path + "/download/%(title)s.%(ext)s";
        Runtime runtime = Runtime.getRuntime();
        Process p = null;
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


    private static void printErrorStream(Process p) throws IOException {
        InputStream is = p.getErrorStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String s = null;
        while ((s = reader.readLine()) != null) {
            System.out.println(s);
        }
        is.close();
    }

    private static double computeHammingDistance2(ArrayList<String> videoHash1, ArrayList<String> videoHash2) {
        ImagePhash imagePhash = new ImagePhash();
        int N1 = videoHash1.size();
        int N2 = videoHash2.size();
        int den = (N1 <= N2) ? N1 : N2;
        int C[][] = new int[N1 + 1][N2 + 1];
        for (int i = 0; i < N1 + 1; i++)
            C[i][0] = 0;

        for (int j = 0; j < N2 + 1; j++)
            C[0][j] = 0;

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
