package vidupe.ffmpeg.phash;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;

@RunWith(Parameterized.class)
public class DissimilarityTest {
    final static String IMG_DIR = "/media/farheen/01D26F1D020D3380/sample/Dissimilarity/test1/";
    final static String IMG_DIR2 = "/media/farheen/01D26F1D020D3380/sample/DissimilarityVideoTest/test1/";

    private File f1;
    private File f2;

    public DissimilarityTest(File f1, File f2) {
        this.f1 = f1;
        this.f2 = f2;
    }

    @Parameterized.Parameters(name = "{index} : {0} {1}")
    public static List<File[]> data() {
        List<File> video1Files = listFiles(IMG_DIR2 + "15", new String[]{"jpg", "jpeg"});
        List<File> video2Files = listFiles(IMG_DIR2 + "3", new String[]{"jpg", "jpeg"});
        List<File[]> result = new ArrayList<>();
        for(File file1 : video1Files) {
            for(File file2: video2Files) {
                result.add(new File[]{file1, file2});
            }
        }
        return result;
    }

    private static List<File> listImageFiles(String directory1) {

        final Collection collection = FileUtils.listFiles(new File(directory1), new String[]{"jpg", "jpeg"}, true);
        List<File> filePaths = new ArrayList<File>();
        filePaths.addAll(collection);
        return filePaths;
    }

    private static List<File> listFiles(String directory1, String[] extensions) {

        final Collection collection = FileUtils.listFiles(new File(directory1), extensions, true);
        List<File> filePaths = new ArrayList<>();
        filePaths.addAll(collection);
        filePaths.sort(new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                String fileName = FilenameUtils.removeExtension(file1.getName());
                int fileNameInt1 = Integer.parseInt(fileName);
                fileName = FilenameUtils.removeExtension(file2.getName());
                int fileNameInt2 = Integer.parseInt(fileName);
                return fileNameInt1 - fileNameInt2;
            }
        });
        return filePaths;
    }


    @Test
    public void testSimilarity() throws Exception {
        ImagePhash phash = new ImagePhash();
        InputStream inputStream1 = new FileInputStream(f1);
        InputStream inputStream2 = new FileInputStream(f2);
        String hash1 = phash.getHash(inputStream1);
        String hash2 = phash.getHash(inputStream2);
        assertEquals(hash2.length(), hash1.length());
        int dist = phash.hammingDistance(hash1 , hash2);
        assertTrue("hammingDistance = " + dist + " is less than 19", dist >=19);
    }
}
