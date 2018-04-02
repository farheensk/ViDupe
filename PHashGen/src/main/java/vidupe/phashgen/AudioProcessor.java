package vidupe.phashgen;

import com.musicg.fingerprint.FingerprintManager;
import com.musicg.wave.Wave;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AudioProcessor {
    private static final Logger logger = LoggerFactory.getLogger(VideoProcessor.class);
    static Runtime runtime = Runtime.getRuntime();
    String videoFilePath;
    File videoFile;
    AudioProcessor(String videoFilePath, File downloadedVideoFile){
        this.videoFilePath = videoFilePath;
        this.videoFile = downloadedVideoFile;
    }
    public byte[] processAudio() {
        logger.info("Analyzing Audio:start");
        extractAudio();
        byte[] audioHashes = generateAudioHashes();
        logger.info("Analyzing Audio:end");
        return audioHashes;
    }

    public void extractAudio() {
        logger.info("Audio extraction:start");
        String fileName1 = FilenameUtils.removeExtension(videoFile.getName());
        String command = "ffmpeg -i " + videoFile+ " -acodec pcm_s16le -ar 44100 -ac 2 " + videoFilePath+fileName1+".wav";
        try {
            Process p = runtime.exec(command);
            p.waitFor();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        logger.info("Audio extraction:end");
    }

    public byte[] generateAudioHashes() {
        logger.info("Generating Audio Hashes:start");
        String fileName1 = FilenameUtils.removeExtension(videoFile.getName());
        byte[] firstFingerPrint = new FingerprintManager().extractFingerprint(new Wave(videoFilePath+ fileName1+".wav"));
        deleteFile();
        logger.info("Generating Audio Hashes:end");
        return firstFingerPrint;
    }
    public void deleteFile() {
        String fileName = FilenameUtils.removeExtension(videoFile.getName());
        File file = new File(videoFilePath+fileName+".wav");
        if (file.exists()) {
            file.delete();
        }
    }
}
