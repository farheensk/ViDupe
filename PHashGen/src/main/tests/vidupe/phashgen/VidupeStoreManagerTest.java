package vidupe.phashgen;

import com.google.api.client.util.DateTime;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import org.junit.After;
import org.junit.Test;
import vidupe.constants.Constants;
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

    public VidupeStoreManagerTest() {
        this.datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        this.vidupeStoreManager = new VidupeStoreManager(datastore);
    }
    @Test
    public void resetEntityProperty() {
    }

    @Test
    public void createKey() {
    }

    @Test
    public void writeInDataStore() {
    }

    @Test
    public void writeHashesInDataStore() {

    }

    @Test
    public void checkIfAllVideosAreProcessed() {
        Datastore datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(datastore);
        String id = getKey();
        Date date = new Date();
        VideoMetaData videoMetaData = createVideoMetaData(id, date);
        Entity entity = createEntity(videoMetaData, CLIENT_ID,true);
        String id2 = getKey();
        VideoMetaData videoMetaData1 = createVideoMetaData(id2, date);
        createEntity(videoMetaData1,CLIENT_ID, true);
        HashGenMessage message = HashGenMessage.builder().videoId(id).email(CLIENT_ID).build();
        final boolean checkIfAllVideosAreProcessed = vidupeStoreManager.checkIfAllVideosAreProcessed(message);
        assertEquals(true,checkIfAllVideosAreProcessed);
        HashGenMessage message1 = HashGenMessage.builder().videoId(id2).email(CLIENT_ID).build();
        vidupeStoreManager.checkIfAllVideosAreProcessed(message1);
        assertEquals(true,checkIfAllVideosAreProcessed);
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
                .set("video-name", videoMetaData.getName())
                .set("duration", videoMetaData.getDuration())
                .set("last-processed", Timestamp.now().getSeconds() * 1000)
                .set("video-last-modified", 12324)
                .set("exists-in-drive", BooleanValue.newBuilder(true).setExcludeFromIndexes(true).build())
                .set("processed", BooleanValue.newBuilder(processed).setExcludeFromIndexes(true).build())
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
        deleteEntity("123456",CLIENT_ID);
        deleteEntity("325",CLIENT_ID);
    }
}