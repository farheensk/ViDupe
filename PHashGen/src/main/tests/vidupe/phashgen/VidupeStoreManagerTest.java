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

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

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
        String id2 = createVidupeEntity(p2);
        HashGenMessage message = HashGenMessage.builder().videoId(id).email(CLIENT_ID).build();
        boolean checkIfAllVideosAreProcessed = vidupeStoreManager.checkIfAllVideosAreProcessed(message);
        assertEquals(p1&&p2 , checkIfAllVideosAreProcessed);
    }

    private String createVidupeEntity(boolean isProcessed) {
        String id = getKey();
        Date date = new Date();
        VideoMetaData videoMetaData = createVideoMetaData(id, date);
        final Entity entity = createEntity(videoMetaData, CLIENT_ID, isProcessed);
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
                .set(EntityProperties.VIDEO_LAST_MODIFIED, 12324)
                .set(EntityProperties.EXISTS_IN_DRIVE, true)
                .set(EntityProperties.PROCESSED, processed)
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

    @After
    public void cleanUp() {
        for(String k : keyList) {
            deleteEntity(k, CLIENT_ID);
        }
    }
}