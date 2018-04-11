package vidupe.phashgen;

import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.constants.EntityProperties;
import vidupe.constants.UserEntityProperties;
import vidupe.message.HashGenMessage;

import java.util.ArrayList;
import java.util.List;

public class VidupeStoreManager {

    private final Datastore datastore;
    private static final Logger logger = LoggerFactory.getLogger(VidupeStoreManager.class);

    public VidupeStoreManager(Datastore dataStore) {
        this.datastore = dataStore;
    }

    public Entity findByKey(String id, String ancestorId) {
        logger.debug("Finding key :" + id);
        Key key = createKey(id, ancestorId);
        Query<Entity> query1 = Query.newEntityQueryBuilder()
                .setFilter(StructuredQuery.PropertyFilter.eq("__key__", key))
                .build();
        QueryResults<Entity> results = this.datastore.run(query1);
        Entity[] entities = Iterators.toArray(results, Entity.class);
        Entity e = null;
        for (Entity e1 : entities) {
            if (e1.getKey().equals(key))
                e = e1;
        }
        return e;
    }

    public void resetVideoEntityPHashgenProperty(Entity e, HashGenMessage message, int keyFramesSize) {
        logger.info("Marking entity processed, key=" + e.getKey());
        Key key = createKey(message.getVideoId(), message.getEmail());
        Entity task = Entity.newBuilder(key)
                .set(EntityProperties.VIDEO_NAME, e.getString(EntityProperties.VIDEO_NAME))
                .set(EntityProperties.DURATION, e.getLong(EntityProperties.DURATION))
                .set(EntityProperties.LAST_PROCESSED, e.getLong(EntityProperties.LAST_PROCESSED))
                .set(EntityProperties.VIDEO_LAST_MODIFIED, e.getLong(EntityProperties.VIDEO_LAST_MODIFIED))
                .set(EntityProperties.EXISTS_IN_DRIVE, e.getBoolean(EntityProperties.EXISTS_IN_DRIVE))
                .set(EntityProperties.DEDUPE_PROCESS, e.getBoolean(EntityProperties.DEDUPE_PROCESS))
                .set(EntityProperties.PHASHGEN_PROCESSED, true)
                .set(EntityProperties.VIDEO_SIZE, e.getLong(EntityProperties.VIDEO_SIZE))
                .set(EntityProperties.NUM_KEYFRAMES, keyFramesSize)
                .build();
        Entity modifiedEntity = datastore.put(task);
        logger.info("Entity marked as processed, key=" + e.getKey());
        logger.debug("Modified Entity = " + printEntity(modifiedEntity));
    }

    private String printEntity(Entity e) {
        StringBuilder builder = new StringBuilder();
        builder.append(e.getString(EntityProperties.VIDEO_NAME)).append(",");
        builder.append(e.getLong(EntityProperties.LAST_PROCESSED)).append(",");
        builder.append(e.getLong(EntityProperties.VIDEO_LAST_MODIFIED)).append(",");
        builder.append(e.getBoolean(EntityProperties.PHASHGEN_PROCESSED));
        return builder.toString();

    }

    public Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("videos")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }

    public boolean writeInDataStore(ArrayList<Long> hashes, byte[] audioHashes, HashGenMessage message) {
        writeHashesInDataStore(hashes, message);
        writeAudioHashesInDataStore(audioHashes, message);
        String videoId = message.getVideoId();
        String email = message.getEmail();
        Entity entity = findByKey(videoId, email);
        if (entity == null) {
            throw new RuntimeException("Failed to fetch entity with videoId=" + videoId + ", email=" + email);
        }
        for(int i=0;i<3;i++){
            resetVideoEntityPHashgenProperty(entity, message, hashes.size());
            Entity newEntity = findByKey(videoId,email);
            if((newEntity.getLong(EntityProperties.NUM_KEYFRAMES) == hashes.size()) &&
                    newEntity.getBoolean(EntityProperties.PHASHGEN_PROCESSED) == true){
                break;
            }
        }

        return checkIfAllVideosAreProcessed(message);
    }

    public void writeAudioHashesInDataStore(byte[] audioHashes, HashGenMessage message) {
        logger.info("Writing Audio hashes to data-store");
        Blob blob = Blob.copyFrom(audioHashes);
        blob.getLength();
        Key key = datastore.newKeyFactory().setKind("audio").addAncestors(PathElement.of(message.getEmail(), message.getVideoId()))
                .newKey(1);
        Entity hashEntity = Entity.newBuilder(key).
                set("value", BlobValue.newBuilder(blob).setExcludeFromIndexes(true).build()).build();
        datastore.put(hashEntity);
        logger.info("Finished writing of audio hashes");
    }

    public boolean checkIfAllVideosAreProcessed(HashGenMessage message) {
        String email = message.getEmail();
        logger.info("Checking if all videos are processed for user=" + email);

        boolean canSend = false;
        for (int i = 0; i < 3; i++) {
            Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(email);
            Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                    .setFilter(
                            StructuredQuery.CompositeFilter.and(
                                    StructuredQuery.PropertyFilter.hasAncestor(ancestorPath),
                                    StructuredQuery.PropertyFilter.eq(EntityProperties.PHASHGEN_PROCESSED, false)))
                    .build();
            QueryResults<Key> results = this.datastore.run(query);
            if (!results.hasNext()) {
                canSend = true;
                logger.info("All videos are processed, user=" + email);
                break;
            }
        }
        logger.debug("Returning canDedupe=" + canSend);
        return canSend;
    }

    public void writeHashesInDataStore(ArrayList<Long> hashes, HashGenMessage message) {
        logger.info("Writing Video hashes to data-store");
        int i = 1;
        List<Entity> hashesList = new ArrayList<>();
        for (long hash : hashes) {
            Key key = datastore.newKeyFactory().setKind("VideoHashes").addAncestors(PathElement.of(message.getEmail(), message.getVideoId()))
                    .newKey(i);
            i++;
            Entity hashEntity = Entity.newBuilder(key).
                    set("value", hash).build();
            hashesList.add(hashEntity);
            if (i % 500 == 0) {
                datastore.put(Iterators.toArray(hashesList.iterator(), Entity.class));
                hashesList = new ArrayList<>();
            }
        }
        datastore.put(Iterators.toArray(hashesList.iterator(), Entity.class));
        logger.info("Completed writing video hashes");
    }

    public ArrayList<String> getAllVideoIdsOfUser(String email) {
        ArrayList<String> videoIds = new ArrayList<>();
        logger.info("Retrieving videos of user=" + email);
            Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(email);
        Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                .setFilter(
                        StructuredQuery.CompositeFilter.and(
                                StructuredQuery.PropertyFilter.hasAncestor(ancestorPath),
                                StructuredQuery.PropertyFilter.eq(EntityProperties.EXISTS_IN_DRIVE, true),
                                StructuredQuery.PropertyFilter.eq(EntityProperties.PHASHGEN_PROCESSED, true)
                        ))
                .build();
            QueryResults<Key> results = this.datastore.run(query);
            while (results.hasNext()){
                videoIds.add(results.next().getName());
            }
        logger.debug("Returning videoIds of user=" + email);
        return videoIds;
    }

    public void resetUserEntityProperty(HashGenMessage hashGenMessage, boolean doneHashgenProcess) {
        Key key = createUserEntityKey(hashGenMessage.getJobId(), hashGenMessage.getEmail());
        Entity entity = findEntityOfUserTask(key);
        Entity task = Entity.newBuilder(key)
                .set(UserEntityProperties.USER_ID, entity.getString(UserEntityProperties.USER_ID))
                .set(UserEntityProperties.NAME, entity.getString(UserEntityProperties.NAME))
                .set(UserEntityProperties.EMAIL_ID, entity.getString(UserEntityProperties.EMAIL_ID))
                .set(UserEntityProperties.TOTAL_VIDEOS, entity.getLong(UserEntityProperties.TOTAL_VIDEOS))
                .set(UserEntityProperties.FILTERED_VIDEOS_COUNT, entity.getLong(UserEntityProperties.FILTERED_VIDEOS_COUNT))
                .set(UserEntityProperties.PHASHGEN, doneHashgenProcess)
                .set(UserEntityProperties.DEDUPE, false)
                .set(UserEntityProperties.CREATED, entity.getTimestamp(UserEntityProperties.CREATED))
                .set(UserEntityProperties.DONE, entity.getBoolean(UserEntityProperties.DONE))
                .build();
        for(int i=0;i<3;i++){
            datastore.put(task);
            Entity newEntity = datastore.get(key);
            if(newEntity.getBoolean(UserEntityProperties.PHASHGEN) == doneHashgenProcess){
                break;
            }
        }
    }

    private Key createUserEntityKey(String jobId, String clientId) {
        Key key = datastore.newKeyFactory()
                .setKind("users")
                .addAncestors(PathElement.of("user", clientId))
                .newKey(jobId);
        return key;
    }

    private Entity findEntityOfUserTask(Key key) {
        Query<Entity> query1 = Query.newEntityQueryBuilder()
                .setFilter(StructuredQuery.PropertyFilter.eq("__key__", key))
                .build();
        QueryResults<Entity> results = datastore.run(query1);
        Entity[] entities = Iterators.toArray(results, Entity.class);
        Entity e = null;
        for (Entity e1 : entities) {
            if (e1.getKey().equals(key))
                e = e1;
        }
        return e;
    }
}