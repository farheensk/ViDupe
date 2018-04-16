package vidupe.phashgen;

import com.google.api.client.util.DateTime;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.constants.Constants;
import vidupe.constants.EntityProperties;
import vidupe.message.HashGenMessage;

import java.io.File;
import java.util.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class VidupeStoreManagerTest {

    private static final String CLIENT_ID = "123456";
    private List<String> keyList = new LinkedList<>();
    private Datastore datastore;
    private VidupeStoreManager vidupeStoreManager;
    private Logger logger = LoggerFactory.getLogger(VidupeStoreManagerTest.class);

    public VidupeStoreManagerTest() {
        this.datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        this.vidupeStoreManager = new VidupeStoreManager(datastore);
    }

    @Test
    public void checkIfAllVideosAreProcessed() {
        checkIfAllVideosAreProcessedInternal(true, true);
        checkIfAllVideosAreProcessedInternal(false, false);
        checkIfAllVideosAreProcessedInternal(true, false);
        checkIfAllVideosAreProcessedInternal(false, true);
    }

    void checkIfAllVideosAreProcessedInternal(boolean p1, boolean p2) {
        Datastore datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(datastore);
        String id = createVidupeEntity(p1);
        HashGenMessage message = HashGenMessage.builder().videoId(id).email(CLIENT_ID).build();
        boolean checkIfAllVideosAreProcessed = vidupeStoreManager.checkIfAllVideosAreProcessed(message);
        assertEquals(p1 && p2, checkIfAllVideosAreProcessed);
    }

    @Test
    public void getAllVideoIdsOfUser() {
        String id;
        for (int i = 0; i < 4; i++) {
           createVidupeEntity(true);
        }
        ArrayList<String> videoIdsOfUser = vidupeStoreManager.getAllVideoIdsOfUser(CLIENT_ID);
        assertTrue(videoIdsOfUser.size() != 0);
    }


    private String createVidupeEntity(boolean isProcessed) {
        String id = getKey();
        Date date = new Date();
        VideoMetaData videoMetaData = createVideoMetaData(id, date);
        Entity entity = createEntity(videoMetaData, CLIENT_ID, isProcessed);
        datastore.add(entity);
        logger.info("Created entity with id = " + id);
        return id;
    }

    private VideoMetaData createVideoMetaData(String id, Date date) {
        return VideoMetaData.builder().videoSize(100L).duration(10L).height(100L).dateModified(new DateTime(date))
                .width(100L).id(id).description("crap").name("test-name").build();
    }

    private String getKey() {
        Random r = new Random();
        int k = r.nextInt(1000);
        String key = String.valueOf(k);
        keyList.add(key);
        return key;
    }

    private Entity createEntity(VideoMetaData videoMetaData, String clientId, boolean processed) {
        Key key = createKey(videoMetaData.getId(), clientId);
        return Entity.newBuilder(key)
                .set(EntityProperties.VIDEO_NAME, videoMetaData.getName())
                .set(EntityProperties.DURATION, videoMetaData.getDuration())
                .set(EntityProperties.LAST_PROCESSED, Timestamp.now().getSeconds() * 1000)
                .set(EntityProperties.DEDUPE_PROCESS, false)
                .set(EntityProperties.VIDEO_LAST_MODIFIED, 12324)
                .set(EntityProperties.EXISTS_IN_DRIVE, true)
                .set(EntityProperties.PHASHGEN_PROCESSED, processed)
                .set(EntityProperties.NUM_KEYFRAMES, 12)
                .build();
    }

    private Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("videos")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }

    void deleteEntity(String keyName, String clientId) {
        Key key = createKey(keyName, clientId);
        this.datastore.delete(key);
    }

    @Test
    public void writeAudioHashesInDataStore() {
        String videFilePath = "/media/farheen/01D26F1D020D3380/CC_WEB_VIDEO/test2/";
        File downloadedFile = new File(videFilePath + "/2_711_H.wmv");
        AudioProcessor audioProcessor = new AudioProcessor(videFilePath, downloadedFile);
        //final byte[] bytes = audioProcessor.processAudio();
        byte[] bytes = new byte[0];
        HashGenMessage message = HashGenMessage.builder().videoId("12345").email("farheen@gmail.com").build();
        vidupeStoreManager.writeAudioHashesInDataStore(bytes, message);
        Key key = datastore.newKeyFactory().setKind("audio").addAncestors(PathElement.of(message.getEmail(), message.getVideoId()))
                .newKey(1);
        Entity entity = datastore.get(key);
        System.out.println(bytes);
        System.out.println("=======");
        Blob value = entity.getBlob("value");
        byte[] bytes1 = value.toByteArray();
        for (byte b : bytes1) {
            System.out.println(b);
        }
        System.out.println(value + "    " + bytes1);
        System.out.println("Bytes written in datastore");
    }


    @After
    public void cleanUp() {
        for (String k : keyList) {
            deleteEntity(k, CLIENT_ID);
        }
    }


}