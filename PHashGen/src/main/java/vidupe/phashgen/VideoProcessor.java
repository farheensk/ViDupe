package vidupe.phashgen;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.FileList;
import vidupe.message.HashGenMessage;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;

public class VideoProcessor {
    public Drive generatePhash(HashGenMessage message) {
        String accessToken = message.getAccessToken();
        GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken);
        Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
                .setApplicationName("Duplicate video Detection").build();
        return drive;
    }

    public ArrayList<String> processVideo(HashGenMessage message, Drive drive)
    {
        ArrayList<String> videoHashes = null;
        try {
            FileList result = drive.files().list().setFields(
                    "files(capabilities/canDownload,id,md5Checksum,mimeType,name,size,videoMediaMetadata,webContentLink)")
                    .execute();
            java.io.File dir = new java.io.File("gmail");
            dir.mkdir();
            URL url = new URL("https://www.googleapis.com/drive/v3/files/" + message.getVideoId() + "?alt=media");
            HttpRequest httpRequestGet = drive.getRequestFactory().buildGetRequest(new GenericUrl(url.toString()));
            httpRequestGet.getHeaders().setRange("bytes=" + 0 + "-");
            System.out.println(httpRequestGet.getHeaders());
            HttpResponse resp = httpRequestGet.execute();
            String pathname = "gmail" ;
            System.out.println(dir.getAbsolutePath());
            OutputStream outputStream = new FileOutputStream(
                    new java.io.File(pathname+"/"+ message.getVideoName()));
            resp.download(outputStream);
            extractKeyFrames(pathname+"/",message.getVideoName());
            deleteFile(pathname+"/",message.getVideoName());
            videoHashes = generateHashes(pathname);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return videoHashes;
    }

    public void deleteFile(String pathname, String video_name) {
        File file = new File(pathname+video_name);
        if (file.exists()) {
            file.delete();
        }
    }

    public void extractKeyFrames(String pathname,String videoName) {
        String command="ffmpeg -i "+pathname+videoName+" -vf select=eq(pict_type\\,PICT_TYPE_I) -vsync vfr "+ pathname +"/thumb2_%04d.jpg -hide_banner -y";
        Runtime runtime = Runtime.getRuntime();
        try {
            Process p = runtime.exec(command);
            p.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public ArrayList<String> generateHashes(String pathname) {

        ImagePhash imgphash = new ImagePhash();
        File directory = new File(pathname);
        ArrayList<String> videoHash1 = new ArrayList<>();
        int numberOfKeyframes = 0;
        StringBuilder video_hashes = new StringBuilder();
        File[] f = directory.listFiles();
        if(f != null && f.length>0)
            for (File file : f) {
                if (file.getName().toLowerCase().endsWith(".jpg")) {
                    try {
                    String image1hash = imgphash.getHash(new FileInputStream(file));
                    videoHash1.add(image1hash);
                    video_hashes.append(image1hash+ "$");
                    deleteFile(pathname, file.getName());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        return videoHash1;
    }
}

