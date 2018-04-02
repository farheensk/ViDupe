package vidupe.phashgen;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import java.io.File;

import static junit.framework.TestCase.assertTrue;

public class AudioProcessorTest {
    String videFilePath ="/media/farheen/01D26F1D020D3380/CC_WEB_VIDEO/test2/";
    File downloadedFile = new File(videFilePath+"/2_711_H.wmv");
    AudioProcessor audioProcessor;
    public AudioProcessorTest(){
       audioProcessor = new AudioProcessor(videFilePath, downloadedFile);
    }

    @Test
    public void extractAudioTest() {
        audioProcessor.extractAudio();
        String fileName1 = FilenameUtils.removeExtension(downloadedFile.getName());
        final File file = new File(videFilePath+fileName1+".wav");
        assertTrue(file.exists());
    }

    @Test
    public void generateAudioHashesTest(){
        String fileName1 = FilenameUtils.removeExtension(downloadedFile.getName());
        byte[] audioHashes = audioProcessor.generateAudioHashes();
        assertTrue(audioHashes!=null);
    }

    @Test
    public void processVideoTest(){
        byte[] audioHashes = audioProcessor.processAudio();
       // byte[] audioHashes = audioProcessor.generateAudioHashes();
        assertTrue(audioHashes!=null);

    }

}