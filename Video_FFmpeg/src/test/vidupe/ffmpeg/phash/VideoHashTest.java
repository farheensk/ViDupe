package vidupe.ffmpeg.phash;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

@RunWith(Parameterized.class)
public class VideoHashTest {

    final static String IMG_DIR = "/media/farheen/01D26F1D020D3380/sample/DissimilarityVideoTest/test1/";

    private File videoFile;

    public VideoHashTest(File videoFile) {
        this.videoFile = videoFile;
    }

    @Parameterized.Parameters(name = "{index} : {2}")
    public static List<File> data() {
        List<File> videoFilesList = listFiles(IMG_DIR);
       return videoFilesList;
    }

    private static List<File> listFiles(String directory1) {

        final Collection collection = FileUtils.listFiles(new File(directory1), new String[]{"mp4", "3gp"}, true);
        List<File> filePaths = new ArrayList<File>();
        filePaths.addAll(collection);
        return filePaths;
    }

    @Test
    public void extractKeyFramesTest() {
        VideoHash videoHash = new VideoHash();
        final String fileName = videoFile.getName();
        videoHash.extractKeyFrames(videoFile);
        String fileNameWithOutExt = FilenameUtils.removeExtension(fileName);
        File f1 = new File(IMG_DIR+fileNameWithOutExt);
        final File[] extractedKeyFrames = f1.listFiles();
        int length = extractedKeyFrames.length;
        assertTrue("key frames not extracted", length > 0);
    }

    @Test
    public void generateHashesTest() {
        VideoHash videoHash = new VideoHash();
        List<String> hashes = videoHash.generateHashes(videoFile);
        assertTrue("hashes not generated" , !(hashes.isEmpty()));
    }



}

