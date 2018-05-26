package vidupe.phashgen;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.message.HashGenMessage;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

@Slf4j
public class VideoProcessor {
    static Runtime runtime = Runtime.getRuntime();

    public Drive getDrive(HashGenMessage message) {
        String accessToken = message.getAccessToken();
        GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken);
        Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
                .setApplicationName("Duplicate video Detection").build();
        return drive;
    }

    public VideoAudioHashes processVideo(HashGenMessage message, Drive drive) {
        VideoAudioHashes videoAudioHashes = null;
        try {
            ArrayList<String> videoHashes = new ArrayList<>();
            String pathname = String.valueOf(System.currentTimeMillis());
            java.io.File dir = new java.io.File(pathname);
            while (true){
                    pathname = String.valueOf(System.currentTimeMillis());
                    dir = new File(pathname);
                    if(!(dir.exists())){
                        boolean ifCreated = createDirectory(dir);
                        if(ifCreated == true)
                              break;
                    }
            }

            URL url = new URL("https://www.googleapis.com/drive/v3/files/" + message.getVideoId() + "?alt=media");
            HttpRequest httpRequestGet = drive.getRequestFactory().buildGetRequest(new GenericUrl(url.toString()));
            httpRequestGet.getHeaders().setRange("bytes=" + 0 + "-");
            log.info(httpRequestGet.getHeaders().toString());
            String extension = FilenameUtils.getExtension(message.getVideoName());
            String videoFileName = "video"+"."+extension;
            String videoPath = pathname + "/" + videoFileName;
            File downloadedVideoFile = new File(videoPath);
            OutputStream outputStream = new FileOutputStream(
                    downloadedVideoFile);
            HttpResponse resp;
            int statusCode;
            try {
                resp = httpRequestGet.execute();
                resp.download(outputStream);

            } finally {
                outputStream.close();
            }
            statusCode = resp.getStatusCode();
            log.info("StatusCode=" + statusCode + ", Status=" + resp.getStatusMessage());
            String keyFramesPath = pathname + "/" + "keyFrames";
            File keyFramesDirectory = new File(keyFramesPath);
            createDirectory(keyFramesDirectory);
            extractKeyFrames(pathname + "/", keyFramesPath, downloadedVideoFile);
            videoHashes = generateHashes(keyFramesPath);
            AudioProcessor audioProcessor = new AudioProcessor(pathname + "/", downloadedVideoFile);
            byte[] audioHashes = audioProcessor.processAudio();
            videoAudioHashes = VideoAudioHashes.builder().videoHashes(videoHashes)
                    .audioHashes(audioHashes).build();
            deleteFile(pathname + "/", videoFileName);
            deleteDirectory(pathname + "/");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return videoAudioHashes;
    }

    private boolean createDirectory(File dir) {
        boolean success = false;
        for (int i = 0; i < 10; i++) {
            success = dir.mkdir();
            if (success) {
                log.info("Created directory " + dir.getName());
                break;
            }
        }
//        if (!success) {
//            throw new RuntimeException("Couldn't create directory " + dir.getName());
//        }
        return success;
    }

    private void deleteDirectory(String path) {
        File directory = new File(path);
        String[] entries = directory.list();
        for (String s : entries) {
            File currentFile = new File(directory.getPath(), s);
            currentFile.delete();
        }
        directory.delete();
    }

    public void deleteFile(String pathname, String video_name) {
        File file = new File(pathname + video_name);
        if (file.exists()) {
            file.delete();
        }
    }

    public void extractKeyFrames(String pathname, String keyFramesPath, File videoName) {
        String command = "ffmpeg -i " + pathname + videoName.getName() + " -vf select=eq(pict_type\\,PICT_TYPE_I) -vsync vfr " + keyFramesPath + "/%04d.jpg -hide_banner -y";
        // Runtime runtime = Runtime.getRuntime();
        try {
            Process p = runtime.exec(command);
            p.waitFor();
            p.getErrorStream();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<String> generateHashes(String pathname) {
        ImagePhash imgphash = new ImagePhash();
        Collection<File> collection = FileUtils.listFiles(new File(pathname), new String[]{"jpg", "jpeg"}, true);
        ArrayList<String> hashes = new ArrayList<>();
        List<File> list = new ArrayList<>(collection);
        list.sort(new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                String fileName1 = FilenameUtils.removeExtension(file1.getName());
                String fileName2 = FilenameUtils.removeExtension(file2.getName());
                int fileNameInt1 = Integer.parseInt(fileName1);
                int fileNameInt2 = Integer.parseInt(fileName2);
                return fileNameInt1 - fileNameInt2;
            }
        });
        log.info("num_key_frames=" + list.size());
        int size = list.size();
        int i = 0;
        for (File file : list) {
            try {
                String imageHash = imgphash.getHash(new FileInputStream(file));
                hashes.add(imageHash);
                i++;
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (i == size) {
                deleteDirectory(pathname);
            }
        }
        return hashes;
    }
}


