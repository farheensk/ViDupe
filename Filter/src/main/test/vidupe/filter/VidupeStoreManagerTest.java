package vidupe.filter;

import com.google.api.client.util.DateTime;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import org.junit.After;
import org.junit.Test;
import vidupe.filter.constants.Constants;
import vidupe.filter.constants.VideoEntityProperties;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class VidupeStoreManagerTest {

    private static final String CLIENT_ID = "123456";
    private static final String CLIENT_ID2 = "client2";
    private Datastore datastore;
    private VidupeStoreManager vidupeStoreManager;
    private List<String> keyList = new LinkedList<>();

    public VidupeStoreManagerTest() {
        this.datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        this.vidupeStoreManager = new VidupeStoreManager(datastore);
    }

    @Test
    public void shouldCreateNewEntityGivenMetaData() {
        String id = getKey();

        VideoMetaData videoMetaData = VideoMetaData.builder().videoSize(100L).duration(10L).height(100L)
                .width(100L).id(id).description("crap").name("test-name").dateModified(DateTime.parseRfc3339("2018-02-25T00:00:01Z")).thumbnailLink("link").build();
        assertNotNull(vidupeStoreManager.createEntity(videoMetaData, CLIENT_ID));
        vidupeStoreManager.deleteEntity(id, CLIENT_ID);
    }


    @Test
    public void shouldUpdateCreateEntityGivenMetaData() {
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(datastore);
        String id = getKey();
        Date date = new Date();
        VideoMetaData videoMetaData = createVideoMetaData(id, date);
        Entity entity = vidupeStoreManager.createEntity(videoMetaData, CLIENT_ID);
        assertNotNull(entity);
        assertEquals(true, entity.getValue(VideoEntityProperties.EXISTS_IN_DRIVE).get());
    }

    private VideoMetaData createVideoMetaData(String id, Date date) {
        return VideoMetaData.builder().videoSize(100L).duration(10L).height(100L).dateModified(new DateTime(date))
                .width(100L).id(id).description("crap").name("test-name").thumbnailLink("link").build();
    }

   /* @Test
    public void shouldCompareGivenTimes() {
        Datastore datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(datastore);
        Date date = new Date();
        assertTrue(vidupeStoreManager.isModified(Timestamp.of(date), new DateTime(date)));
    }*/

    @Test
    public void resetEntityPropertyTest(){
        String videoMetaId = getKey();
        Date date = new Date();
        VideoMetaData videoMetaData = createVideoMetaData(videoMetaId, date);
        Entity entity = vidupeStoreManager.createEntity(videoMetaData, CLIENT_ID);
        vidupeStoreManager.resetEntityProperty(entity, videoMetaData,false);
        Entity video = vidupeStoreManager.findByKey(videoMetaId, CLIENT_ID);
        assertEquals(false, video.getValue(VideoEntityProperties.EXISTS_IN_DRIVE).get());
        vidupeStoreManager.resetEntityProperty(entity, videoMetaData,true);
        video = vidupeStoreManager.findByKey(videoMetaId, CLIENT_ID);
        assertEquals(true, video.getValue(VideoEntityProperties.EXISTS_IN_DRIVE).get());
    }

    @Test
    public void deleteAllEntitiesIfNotExistsInDriveTest() {
        String videoId1 = getKey();
        VideoMetaData videoMetaData = createVideoMetaData(videoId1, new Date());
        VideoMetaData videoMetaData1 = createVideoMetaData(getKey(), new Date());
        createEntity(videoMetaData, CLIENT_ID, false);
        createEntity(videoMetaData1, CLIENT_ID, false);
        videoMetaData = createVideoMetaData(videoId1, new Date());
        videoMetaData1 = createVideoMetaData(getKey(), new Date());
        createEntity(videoMetaData, CLIENT_ID2, true);
        createEntity(videoMetaData1, CLIENT_ID2, true);
        vidupeStoreManager.deleteAllEntitiesIfNotExistsInDrive(CLIENT_ID2);
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(CLIENT_ID);
        Query<Entity> query = Query.newEntityQueryBuilder().setKind("videos")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .build();
        QueryResults<Entity> result = this.datastore.run(query);
        Entity[] entities = Iterators.toArray(result, Entity.class);
        for(Entity e: entities){
            System.out.println(e);
        }


    }

    public Entity createEntity(VideoMetaData videoMetaData, String ancestorId, boolean ifExists) {
        Entity entity = fillEntity(videoMetaData, ancestorId, ifExists);
        return datastore.add(entity);
    }
    private Entity fillEntity(VideoMetaData videoMetaData, String ancestorId, boolean ifExists) {

        Key key = createKey(videoMetaData.getId(), ancestorId);
        return Entity.newBuilder(key)
                .set(VideoEntityProperties.VIDEO_NAME, videoMetaData.getName())
                .set(VideoEntityProperties.DURATION, videoMetaData.getDuration())
                .set(VideoEntityProperties.LAST_PROCESSED, Timestamp.now().getSeconds() * 1000)
                .set(VideoEntityProperties.VIDEO_LAST_MODIFIED, 12342)
                .set(VideoEntityProperties.EXISTS_IN_DRIVE, ifExists)
                .set(VideoEntityProperties.PROCESSED, false)
                .set(VideoEntityProperties.THUMBNAIL_LINK, videoMetaData.getThumbnailLink())
                .build();
    }
    public Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("videos")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }
    @Test
    public void sortByIdTest(){
        String videoMetaId1 = "999";
        String videoMetaId2 = getKey();
        VideoMetaData videoMetaData1 = createVideoMetaData(videoMetaId1, new Date());
        VideoMetaData videoMetaData2 = createVideoMetaData(videoMetaId2, new Date());
        Entity entity1 = vidupeStoreManager.createEntity(videoMetaData1, CLIENT_ID);
        Entity entity2 = vidupeStoreManager.createEntity(videoMetaData2, CLIENT_ID);

        Query<Entity> query = Query.newEntityQueryBuilder().setKind("videos")
                .setOrderBy(StructuredQuery.OrderBy.desc("__key__"))
                .build();
        QueryResults<Entity> result = this.datastore.run(query);
        Entity[] entities = Iterators.toArray(result, Entity.class);
        for(Entity e: entities){
            System.out.println(e);
        }
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