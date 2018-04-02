package vidupe.phashgen;

import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.constants.EntityProperties;
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
        for(Entity e1: entities){
            if(e1.getKey().equals(key))
                e = e1;
        }
        return e;
    }

    public void resetEntityProperty(Entity e, HashGenMessage message, int keyFramesSize) {
        logger.info("Marking entity processed, key=" + e.getKey());
        Key key = createKey(message.getVideoId(), message.getEmail());
        Entity task = Entity.newBuilder(key)
                .set(EntityProperties.VIDEO_NAME, e.getString(EntityProperties.VIDEO_NAME))
                .set(EntityProperties.DURATION, e.getLong(EntityProperties.DURATION))
                .set(EntityProperties.LAST_PROCESSED, e.getLong(EntityProperties.LAST_PROCESSED))
                .set(EntityProperties.VIDEO_LAST_MODIFIED, e.getLong(EntityProperties.VIDEO_LAST_MODIFIED))
                .set(EntityProperties.EXISTS_IN_DRIVE, e.getBoolean(EntityProperties.EXISTS_IN_DRIVE))
                .set(EntityProperties.PROCESSED, true)
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
        builder.append(e.getBoolean(EntityProperties.PROCESSED));
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
        if(entity == null) {
            throw new RuntimeException("Failed to fetch entity with videoId=" + videoId + ", email=" + email);
        }
        resetEntityProperty(entity, message, hashes.size());
        return checkIfAllVideosAreProcessed(message);
    }

    private void writeAudioHashesInDataStore(byte[] audioHashes, HashGenMessage message) {
        logger.info("Writing Audio hashes to data-store");
        Blob blob = Blob.copyFrom(audioHashes);
        blob.getLength();
        Key key = datastore.newKeyFactory().setKind("audio").addAncestors(PathElement.of(message.getEmail(), message.getVideoId()))
                .newKey(1);
        Entity hashEntity = Entity.newBuilder(key).
                set("value", blob).build();
        datastore.put(hashEntity);
        logger.info("Finished writing of audio hashes");
    }

    public boolean checkIfAllVideosAreProcessed(HashGenMessage message) {
        String email = message.getEmail();
        logger.info("Checking if all videos are processed for user=" + email);

        boolean canSend=false;
        for(int i=0; i<3; i++) {
            Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(email);
            Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                    .setFilter(
                            StructuredQuery.CompositeFilter.and(
                                    StructuredQuery.PropertyFilter.hasAncestor(ancestorPath),
                                    StructuredQuery.PropertyFilter.eq(EntityProperties.PROCESSED, false)))
                    .build();
            QueryResults<Key> results = this.datastore.run(query);
            if(!results.hasNext()) {
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
        int i=1;
        List<Entity> hashesList = new ArrayList<>();
        for(long hash: hashes){
            Key key = datastore.newKeyFactory().setKind("hashes").addAncestors(PathElement.of(message.getEmail(), message.getVideoId()))
                    .newKey(i);
            i++;
            Entity hashEntity = Entity.newBuilder(key).
                    set("value", hash).build();
            hashesList.add(hashEntity);
            if(i%500 == 0){
                datastore.put(Iterators.toArray(hashesList.iterator(), Entity.class));
                hashesList = new ArrayList<>();
            }
        }
        datastore.put(Iterators.toArray(hashesList.iterator(), Entity.class));
        logger.info("Completed writing video hashes");
    }


}