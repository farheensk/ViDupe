package vidupe.ffmpeg.phash;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;

public class DeSerialize {
    public static void main(String[] args) {
        String path = "/media/farheen/01D26F1D020D3380/sample/test1";
        HashMap<String, ArrayList<String>> videoHashes = new HashMap<>();
        try {
            FileInputStream fileIn = new FileInputStream(path + "/ImagePhash.ser");
            ObjectInputStream inputStream = new ObjectInputStream(fileIn);
            videoHashes = (HashMap<String, ArrayList<String>>) inputStream.readObject();
            inputStream.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        process(videoHashes);
        //System.out.println("Hashes" + videoHashes);
        System.out.println(videoHashes.size());
//        int count = 0;
//        for(Map.Entry<String, ArrayList<String>> en: videoHashes.entrySet()) {
//            if(count <= 100)
//                System.out.println(en.getValue());
//            count ++;
//            //  System.out.println(en.getValue());
//        }
    }

    private static void process(HashMap<String, ArrayList<String>> videoHashes) {
        HashMap<String ,ArrayList<String>> videoHashes2 = new HashMap<>();
        videoHashes2.putAll(videoHashes);
        ArrayList<String> ids = new ArrayList<>();
        boolean toRemove = true;
        for(ArrayList<String> list: videoHashes.values()){
            ids.add(list.get(0));
            list.remove(0);

        }
        System.out.println(ids);
        int i = 0;
        int j = 0;
        for(ArrayList<String> list1  : videoHashes.values()){
            String videoId1 = ids.get(i);
            i++;
            for(ArrayList<String> list2 : videoHashes2.values()){
                String videoId2 = ids.get(j);
                j++;
                if((videoId1.equals("S847I4zoWGk"))){
                    System.out.print(videoId2 + " ");
                    //list2.remove(0);
                    double distance = computeHammingDistance(list1, list2);
                    System.out.println(distance);
                }
            }
            j=0;
        }
//        for (Map.Entry<String, ArrayList<String>> video1 : videoHashes.entrySet()) {
//            ArrayList<String> hashesList1 = video1.getValue();
//            String videoId1 = hashesList1.get(0);
//            hashesList1.remove(0);
//            for (Map.Entry<String, ArrayList<String>> video2 : videoHashes.entrySet()) {
//                ArrayList<String> hashesList2 = video2.getValue();
//                if (videoId1.equals("PuctcXmZ270")) {
//                    System.out.print(video2.getKey()  + " ");
//                    //hashesList2.remove(0);
//                    double distance = computeHammingDistance(hashesList1, hashesList2);
//                    System.out.println(distance);
//                }
//            }
//        }
    }

    private static double computeHammingDistance(ArrayList<String> videoHash1, ArrayList<String> videoHash2) {
        PHash2 imagePhash = new PHash2();
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
                int distance = imagePhash.distance(videopHash1, videopHash2);
                if (distance <= 21) {
                    //System.out.println("[" + (i-1) + ", " + (j-1) + "] = " + distance);
                    C[i][j] = C[i - 1][j - 1] + 1;

                } else {
                    C[i][j] = ((C[i - 1][j] >= C[i][j - 1])) ? C[i - 1][j] : C[i][j - 1];
                }
            }
        }
        double result = (double) (C[N1][N2]) / (double) (den);

        return result;
    }

    private static double computeHammingDistance2(ArrayList<String> videoHash1, ArrayList<String> videoHash2) {
        ImagePhash imagePhash = new ImagePhash();
        int counter = 0;
        int N1 = videoHash1.size();
        int N2 = videoHash2.size();
        int den = (N1 <= N2) ? N1 : N2;
        int i;
        int j;
        int previousj = 0;
        for (i = 0; i < N1; i++) {
            String videopHash1 = videoHash1.get(i);
            for (j = previousj; j < N2; j++) {
                String videopHash2 = videoHash2.get(j);
                int distance = imagePhash.distance(videopHash1, videopHash2);
                if (distance <= 26) {
                    //System.out.println("[" + (i-1) + ", " + (j-1) + "] = " + distance);
                    counter++;
                    previousj = j+1;
                    break;
                }
            }
        }
        double result = (double)counter/(double)den;
        //System.out.println(counter);
            return result;
        }

    }


