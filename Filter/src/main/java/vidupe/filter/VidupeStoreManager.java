package vidupe.filter;

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

    public Entity createEntity(VideoMetaData videoMetaData, String ancestorId) {
        Entity entity = fillEntity(videoMetaData, ancestorId);
        return datastore.add(entity);
    }

    public Entity findByKey(String id, String ancestorId) {
//        System.out.println("Finding key :" + id);
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

    public boolean isModified(Entity vidupeEntity, VideoMetaData videoMetaData) {
        if(videoMetaData.getDateModified() != null){
            long vidupeLastModified = vidupeEntity.getLong(EntityProperties.VIDEO_LAST_MODIFIED);
            long driveLastModified = videoMetaData.getDateModified().getValue();
            return vidupeLastModified < driveLastModified;
        }
        else{
            return false;
        }

    }

    void deleteEntity(String keyName, String clientId) {
        Key key = createKey(keyName, clientId);
        this.datastore.delete(key);
    }

    public Entity putEntity(VideoMetaData videoMetaData, String kind) {
        Entity entity = fillEntity(videoMetaData, kind);
        return datastore.put(entity);
    }

    private Entity fillEntity(VideoMetaData videoMetaData, String ancestorId) {

        Key key = createKey(videoMetaData.getId(), ancestorId);
        long videoLastModified = getVideoLastModified(videoMetaData);
        return Entity.newBuilder(key)
                .set(EntityProperties.VIDEO_NAME, videoMetaData.getName())
                .set(EntityProperties.DURATION, videoMetaData.getDuration())
                .set(EntityProperties.LAST_PROCESSED, Timestamp.now().getSeconds()*1000)
                .set(EntityProperties.VIDEO_LAST_MODIFIED, videoLastModified)
                .set(EntityProperties.HASHES, "")
                .set(EntityProperties.EXISTS_IN_DRIVE, BooleanValue.newBuilder(true).setExcludeFromIndexes(true).build())
                .set(EntityProperties.PROCESSED, BooleanValue.newBuilder(false).setExcludeFromIndexes(true).build())
                .build();
    }

    private long getVideoLastModified(VideoMetaData videoMetaData) {
        long videoLastModified;
        if(videoMetaData.getDateModified() != null)
            videoLastModified = videoMetaData.getDateModified().getValue();
        else
            videoLastModified = Timestamp.now().getSeconds()*1000;
        return videoLastModified;
    }

    public Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("videos")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }

    public void resetEntityProperty(Entity e, VideoMetaData videoMetaData, boolean value) {
        long videoLastModified = getVideoLastModified(videoMetaData);
        Entity task = Entity.newBuilder(e.getKey())
                .set(EntityProperties.VIDEO_NAME, e.getString(EntityProperties.VIDEO_NAME))
                .set(EntityProperties.DURATION, e.getLong(EntityProperties.DURATION))
                .set(EntityProperties.LAST_PROCESSED, e.getLong(EntityProperties.LAST_PROCESSED))
                .set(EntityProperties.VIDEO_LAST_MODIFIED, videoLastModified)
                .set(EntityProperties.HASHES, e.getString(EntityProperties.HASHES))
                .set(EntityProperties.EXISTS_IN_DRIVE, value)
                .set(EntityProperties.PROCESSED, e.getBoolean(EntityProperties.PROCESSED))
                .build();
        datastore.put(task);
    }
}