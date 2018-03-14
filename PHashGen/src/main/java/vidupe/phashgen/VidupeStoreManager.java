package vidupe.phashgen;

import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import vidupe.constants.EntityProperties;
import vidupe.constants.UserEntityProperties;
import vidupe.message.HashGenMessage;

import java.util.ArrayList;
import java.util.List;

public class VidupeStoreManager {

    private final Datastore datastore;

    public VidupeStoreManager(Datastore dataStore) {
        this.datastore = dataStore;
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

    public void resetEntityProperty(Entity e) {
        Entity task = Entity.newBuilder(e.getKey())
                .set(EntityProperties.VIDEO_NAME, e.getString(EntityProperties.VIDEO_NAME))
                .set(EntityProperties.DURATION, e.getLong(EntityProperties.DURATION))
                .set(EntityProperties.LAST_PROCESSED, e.getLong(EntityProperties.LAST_PROCESSED))
                .set(EntityProperties.VIDEO_LAST_MODIFIED, e.getLong(EntityProperties.VIDEO_LAST_MODIFIED))
                .set(EntityProperties.EXISTS_IN_DRIVE, BooleanValue.newBuilder(e.getBoolean(EntityProperties.EXISTS_IN_DRIVE)).setExcludeFromIndexes(true).build())
                .set(EntityProperties.PROCESSED, BooleanValue.newBuilder(true).setExcludeFromIndexes(true).build())
                .build();
        datastore.put(task);
    }

    public Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("videos")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }

    public void writeInDataStore(ArrayList<Long> hashes, HashGenMessage message) {
        final Entity entity = findByKey(message.getVideoId(), message.getEmail());
        writeHashesInDataStore(hashes, message);
        updateUserEntityPropert(message);
        resetEntityProperty(entity);
    }

    private void updateUserEntityPropert(HashGenMessage message) {
        Key key = datastore.newKeyFactory().setKind("users").newKey(message.getEmail());
        Entity entity = datastore.get(key);
        long videosProcessed = entity.getLong(UserEntityProperties.VIDEOS_PROCESSED);
        long totalProcessed = entity.getLong(UserEntityProperties.TOTAL_VIDEOS);
        boolean done = false;
        final long updatedVideoProcessed = videosProcessed + 1;
        if(updatedVideoProcessed == totalProcessed){
             done = true;
        }
        Entity task = Entity.newBuilder(key)
                .set(UserEntityProperties.USER_ID,entity.getString(UserEntityProperties.USER_ID))
                .set(UserEntityProperties.NAME, entity.getString(UserEntityProperties.NAME))
                .set(UserEntityProperties.EMAIL_ID,entity.getString(UserEntityProperties.EMAIL_ID))
                .set(UserEntityProperties.TOTAL_VIDEOS, entity.getLong(UserEntityProperties.TOTAL_VIDEOS))
                .set(UserEntityProperties.VIDEOS_PROCESSED, updatedVideoProcessed)
                .set(UserEntityProperties.CREATED, entity.getTimestamp(UserEntityProperties.CREATED))
                .set(UserEntityProperties.DONE, done)
                .build();
        datastore.put(task);
    }

    public void writeHashesInDataStore(ArrayList<Long> hashes, HashGenMessage message) {
        int i=1;
        List<Entity> hashesList = new ArrayList<>();
        for(long hash: hashes){
            Key key = datastore.newKeyFactory().setKind("hashes").addAncestors(PathElement.of(message.getEmail(), message.getVideoId()))
                    .newKey(i);
            i++;
            Entity hashEntity = Entity.newBuilder(key).
                    set("value", hash).build();
            hashesList.add(hashEntity);
        }
        datastore.put(Iterators.toArray(hashesList.iterator(), Entity.class));
    }
}