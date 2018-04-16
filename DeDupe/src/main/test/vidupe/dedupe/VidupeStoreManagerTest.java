package vidupe.dedupe;

import com.google.cloud.datastore.*;
import org.junit.After;
import org.junit.Test;
import vidupe.constants.Constants;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class VidupeStoreManagerTest {
    private Datastore datastore;
    private VidupeStoreManager vidupeStoreManager;
    private static final String CLIENT_ID = "123456";
    private List<String> keyList = new LinkedList<>();

    public VidupeStoreManagerTest() {
        this.datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        this.vidupeStoreManager = new VidupeStoreManager(datastore);
    }


    @Test
    public void audioHashesFromBlob() {
        Entity[] entities ;
        String videoId = getKey();
        Key key1 = datastore.newKeyFactory().setKind("audio").addAncestors(PathElement.of(CLIENT_ID, videoId))
                .newKey(1);
        byte[] data1 = new byte[] {97, 98, 99, -128};
        Entity entity1 = createEntity(data1, key1);
        Key key2 = datastore.newKeyFactory().setKind("audio").addAncestors(PathElement.of(CLIENT_ID, videoId))
                .newKey(2);
        byte[] data2 = new byte[] {9, 8, 9, -18};
        Entity entity2 = createEntity(data2, key2);
        Key videoKey = datastore.newKeyFactory().setKind("audio").newKey(videoId);
        entities = vidupeStoreManager.retrieveAudioEntityInformation(videoKey, CLIENT_ID);
        vidupeStoreManager.audioHashesFromBlob(entities);
    }
    public Entity createEntity(byte[] data, Key key) {
        Blob blob = Blob.copyFrom(data);
        Entity hashEntity = Entity.newBuilder(key).
                set("value", BlobValue.newBuilder(blob).setExcludeFromIndexes(true).build()).build();
        return datastore.add(hashEntity);
    }

    public Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("videos")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }
    private String getKey() {
        Random r = new Random();
        int k = r.nextInt(1000);
        String key = String.valueOf(k);
        keyList.add(key);
        return key;
    }
    @After
    public void cleanUp() {
        for(String k : keyList) {
            deleteEntity(k, CLIENT_ID);
        }
    }

    void deleteEntity(String keyName, String clientId) {
        Key key = createKey(keyName, clientId);
        this.datastore.delete(key);
    }

    @Test
    public void intraComparison() {
        vidupeStoreManager.intraComparison(null, Constants.THRESHOLD);
    }

    @Test
    public void getVideoHashEntities() {
        String videoId = getKey();
        Key key1 = datastore.newKeyFactory().setKind("VideoHashes").addAncestors(PathElement.of(CLIENT_ID, videoId))
                .newKey(1);
        vidupeStoreManager.getVideoHashEntities(CLIENT_ID,key1);
    }

    @Test
    public void resetVideoEntityDeDupeProperty() {
    }
}