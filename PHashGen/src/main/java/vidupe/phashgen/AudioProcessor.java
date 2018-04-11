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
    //private File file;

    AudioProcessor(String videoFilePath, File downloadedVideoFile) {
        this.videoFilePath = videoFilePath;
        this.videoFile = downloadedVideoFile;
    }

    public byte[] processAudio() {
        logger.info("Analyzing Audio:start");
        boolean extractionSuccess = extractAudio();
        if(extractionSuccess){
            byte[] audioHashes = generateAudioHashes();
            return audioHashes;
        }
        logger.info("Analyzing Audio:end");
       return new byte[0];
    }

    public boolean extractAudio() {
        logger.info("Audio extraction:start");
        String fileName1 = FilenameUtils.removeExtension(videoFile.getName());
        boolean ifAudioExtracted = false;

        String command = "ffmpeg -i " + videoFile + " -acodec pcm_s16le -ar 44100 -ac 2 " + videoFilePath + fileName1 + ".wav";
        try {
            Process p = runtime.exec(command);
            p.waitFor();
            File file = new File(videoFilePath + fileName1 + ".wav");
            if(file.exists()){
                ifAudioExtracted = true;
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        logger.info("Audio extraction:end");
        return ifAudioExtracted;
    }

    public byte[] generateAudioHashes() {
        logger.info("Generating Audio Hashes:start");
        String fileName1 = FilenameUtils.removeExtension(videoFile.getName());
        byte[] firstFingerPrint = new FingerprintManager().extractFingerprint(new Wave(videoFilePath + fileName1 + ".wav"));
        deleteFile();
        logger.info("Generating Audio Hashes:end");
        return firstFingerPrint;
    }

    public void deleteFile() {
        String fileName = FilenameUtils.removeExtension(videoFile.getName());
        File file = new File(videoFilePath + fileName + ".wav");
        logger.info("Deleting audio file:start");
        if (file.exists()) {
            file.delete();
        }
        logger.info("Deleting audio file:start");
    }
}
