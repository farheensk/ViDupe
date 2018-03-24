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

    public void resetEntityProperty(Entity e) {
        Entity task = Entity.newBuilder(e.getKey())
                .set(EntityProperties.VIDEO_NAME, e.getString(EntityProperties.VIDEO_NAME))
                .set(EntityProperties.DURATION, e.getLong(EntityProperties.DURATION))
                .set(EntityProperties.LAST_PROCESSED, e.getLong(EntityProperties.LAST_PROCESSED))
                .set(EntityProperties.VIDEO_LAST_MODIFIED, e.getLong(EntityProperties.VIDEO_LAST_MODIFIED))
                .set(EntityProperties.EXISTS_IN_DRIVE, e.getBoolean(EntityProperties.EXISTS_IN_DRIVE))
                .set(EntityProperties.PROCESSED, true)
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

    public boolean writeInDataStore(ArrayList<Long> hashes, HashGenMessage message) {
        final Entity entity = findByKey(message.getVideoId(), message.getEmail());
        writeHashesInDataStore(hashes, message);
       // boolean canSendMessage = updateUserEntityProperty(message);
        resetEntityProperty(entity);
        boolean canSendMessage = checkIfAllVideosAreProcessed(message);
        logger.info("Message to DeDupe"+canSendMessage);
        return canSendMessage;
    }

    public boolean checkIfAllVideosAreProcessed(HashGenMessage message) {

        boolean canSend=false;
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(message.getEmail());
        Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .setFilter(StructuredQuery.PropertyFilter.eq(EntityProperties.PROCESSED, false))
                .build();
        QueryResults<Key> results = this.datastore.run(query);


        if(!results.hasNext()) {
            canSend = true;
        }

        logger.debug("Returning canSend=" + canSend);

        return canSend;
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