package vidupe.filter;

import com.google.api.client.util.DateTime;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import vidupe.filter.constants.EntityProperties;

public class VidupeStoreManager {

    private final Datastore datastore;

    public VidupeStoreManager(Datastore dataStore) {
        this.datastore = dataStore;
    }

    public void deleteAllEntitiesIfNotExistsInDrive(String ancestorId) {
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(ancestorId);
        Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .setFilter(StructuredQuery.PropertyFilter.eq(EntityProperties.EXISTS_IN_DRIVE, false))
                .build();
        QueryResults<Key> result = this.datastore.run(query);
        this.datastore.delete(Iterators.toArray(result, Key.class));
    }

    public Entity createEntity(VideoMetaData videoMetaData, String kind) {
        Entity entity = fillEntity(videoMetaData, kind);
        return datastore.add(entity);
    }

    public Entity findByKey(String id, String ancestorId) {
        System.out.println("Finding key :" + id);
        Key key = createKey(id, ancestorId);

        Query<Entity> query1 = Query.newEntityQueryBuilder()
                .setFilter(StructuredQuery.PropertyFilter.eq("__key__", key))
                .build();
        QueryResults<Entity> results = this.datastore.run(query1);
        Entity[] entities = Iterators.toArray(results, Entity.class);
        Entity e = null;
        for(Entity e1: entities){
            if(e1.getKey().equals(key))
                e = e1;
        }
        return e;
    }

    boolean compareTimes(Timestamp created, DateTime dateModified) {
        long modifiedMillis = dateModified.getValue();
        long createdMillis = created.getSeconds() * 1000;
        return createdMillis < modifiedMillis;
    }

    public boolean compareEntityTimes(Entity video, VideoMetaData videoMetaData) {
        Timestamp created = video.getTimestamp(EntityProperties.CREATED);
        DateTime dateModified = videoMetaData.getDateModified();
        return compareTimes(created, dateModified);
    }

    void deleteEntity(String keyName, String clientId) {
        Key key = createKey(keyName, clientId);
        this.datastore.delete(key);
    }

    public Entity putEntity(VideoMetaData videoMetaData, String kind) {
        Entity entity = fillEntity(videoMetaData, kind);
        return datastore.put(entity);
    }

    private Entity fillEntity(VideoMetaData videoMetaData, String kind) {

        Key key = createKey(videoMetaData.getId(), kind);

        return Entity.newBuilder(key)
                .set(EntityProperties.VIDEO_NAME, videoMetaData.getName())
                .set(EntityProperties.DURATION, videoMetaData.getDuration())
                .set(EntityProperties.CREATED, Timestamp.now())
                .set(EntityProperties.HASHES, "")
                .set(EntityProperties.EXISTS_IN_DRIVE, BooleanValue.newBuilder(true).setExcludeFromIndexes(true).build())
                .set(EntityProperties.PROCESSED, BooleanValue.newBuilder(false).setExcludeFromIndexes(true).build())
                .build();
    }

    public Key createKey(String keyName, String clientId) {
        Key key = datastore.newKeyFactory()
                .setKind("videos")
                .addAncestors(PathElement.of("user", clientId))
                .newKey(keyName);
        return key;
    }

    public void resetEntityProperty(Entity e, String property, boolean value) {
        Entity newEntity = Entity.newBuilder(e).set(property, value).build();
        datastore.update(newEntity);
    }
}