package vidupe.filter;

import com.google.api.client.util.DateTime;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import org.junit.After;
import org.junit.Test;
import vidupe.filter.constants.Constants;
import vidupe.filter.constants.EntityProperties;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class VidupeStoreManagerTest {

    private static final String CLIENT_ID = "123456";
    private Datastore datastore;
    private VidupeStoreManager vidupeStoreManager;
    private List<String> keyList = new LinkedList<>();

    public VidupeStoreManagerTest() {
        this.datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        this.vidupeStoreManager = new VidupeStoreManager(datastore);
    }

    @Test
    public void shouldCreateNewEntityGivenMetaData() {
        VideoMetaDataBuilder builder = new VideoMetaDataBuilder();
        String id = getKey();

        VideoMetaData videoMetaData = builder.videoSize(100L).duration(10L).height(100L)
                .width(100L).id(id).description("crap").name("test-name").build();
        assertNotNull(vidupeStoreManager.createEntity(videoMetaData, CLIENT_ID));
        vidupeStoreManager.deleteEntity(id, CLIENT_ID);
    }


    @Test
    public void shouldUpdateCreateEntityGivenMetaData() {
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(datastore);
        VideoMetaDataBuilder builder = new VideoMetaDataBuilder();
        String id = getKey();
        Date date = new Date();
        VideoMetaData videoMetaData = createVideoMetaData(builder, id, date);
        Entity entity = vidupeStoreManager.createEntity(videoMetaData, CLIENT_ID);
        assertNotNull(entity);
        assertEquals(true, entity.getValue(EntityProperties.EXISTS_IN_DRIVE).get());
    }

    private VideoMetaData createVideoMetaData(VideoMetaDataBuilder builder, String id, Date date) {
        return builder.videoSize(100L).duration(10L).height(100L).dateModified(new DateTime(date))
                .width(100L).id(id).description("crap").name("test-name").build();
    }

    @Test
    public void shouldCompareGivenTimes() {
        Datastore datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(datastore);
        Date date = new Date();
        assertTrue(vidupeStoreManager.compareTimes(Timestamp.of(date), new DateTime(date)));
    }

    @Test
    public void resetEntityPropertyTest(){
        VideoMetaDataBuilder builder = new VideoMetaDataBuilder();
        String videoMetaId = getKey();
        Date date = new Date();
        VideoMetaData videoMetaData = createVideoMetaData(builder, videoMetaId, date);
        Entity entity = vidupeStoreManager.createEntity(videoMetaData, CLIENT_ID);
        vidupeStoreManager.resetEntityProperty(entity, EntityProperties.EXISTS_IN_DRIVE,false);
        Entity video = vidupeStoreManager.findByKey(videoMetaId, CLIENT_ID);
        assertEquals(false, video.getValue(EntityProperties.EXISTS_IN_DRIVE).get());
        vidupeStoreManager.resetEntityProperty(entity, EntityProperties.EXISTS_IN_DRIVE,true);
        video = vidupeStoreManager.findByKey(videoMetaId, CLIENT_ID);
        assertEquals(true, video.getValue(EntityProperties.EXISTS_IN_DRIVE).get());
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
            vidupeStoreManager.deleteEntity(k, CLIENT_ID);
        }
    }

}