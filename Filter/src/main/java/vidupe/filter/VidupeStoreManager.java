package vidupe.filter;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.filter.constants.Constants;
import vidupe.filter.constants.UserEntityProperties;
import vidupe.filter.constants.VideoEntityProperties;

import java.util.ArrayList;

public class VidupeStoreManager {
    private static final Logger logger = LoggerFactory.getLogger(VidupeMessageProcessor.class);
    private final Datastore datastore;

    public VidupeStoreManager(Datastore dataStore) {
        this.datastore = dataStore;
    }

    public void deleteAllEntitiesIfNotExistsInDrive(String ancestorId) {
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(ancestorId);
        Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                .setFilter(
                        StructuredQuery.CompositeFilter.and(
                                StructuredQuery.PropertyFilter.hasAncestor(ancestorPath),
                                StructuredQuery.PropertyFilter.eq(VideoEntityProperties.EXISTS_IN_DRIVE, false)))
                .build();
        QueryResults<Key> result = this.datastore.run(query);
        Key[] keys = Iterators.toArray(result, Key.class);
        deleteHashesRelatedToEntity(keys, ancestorId);
        this.datastore.delete(keys);
    }

    public void deleteHashesRelatedToEntity(Key[] result, String ancestorId) {
        Key ancestorPath;
        if (result != null) {
            for (Key key : result) {
                ancestorPath = datastore.newKeyFactory().setKind(ancestorId).newKey(key.getName());
                Query<Key> query = Query.newKeyQueryBuilder().setKind("VideoHashes")
                        .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath)).build();
                QueryResults<Key> hashesResult = this.datastore.run(query);
                this.datastore.delete(Iterators.toArray(hashesResult, Key.class));
            }
        }
    }

    public Entity createEntity(VideoMetaData videoMetaData, String ancestorId) {
        Entity entity = fillEntity(videoMetaData, ancestorId);
        return datastore.add(entity);
    }

    public Entity findByKey(String id, String ancestorId) {
        logger.info("Finding key :" + id);
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

    public boolean isModified(Entity vidupeEntity, VideoMetaData videoMetaData) {
        if (videoMetaData.getDateModified() != null) {
            long vidupeLastModified = vidupeEntity.getLong(VideoEntityProperties.VIDEO_LAST_MODIFIED);
            long driveLastModified = videoMetaData.getDateModified().getValue();
            return vidupeLastModified < driveLastModified;
        } else {
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
        boolean phashgenProcessed = false;
        boolean dedupeProcessed = false;
        long numKeyFrames = 0;
        if (videoMetaData.getVideoSize() > 500000000) {
            phashgenProcessed = true;
            dedupeProcessed = true;
            numKeyFrames = -1;
        }
        Key key = createKey(videoMetaData.getId(), ancestorId);
        long videoLastModified = getVideoLastModified(videoMetaData);
        return Entity.newBuilder(key)
                .set(VideoEntityProperties.VIDEO_NAME, videoMetaData.getName())
                .set(VideoEntityProperties.DURATION, videoMetaData.getDuration())
                .set(VideoEntityProperties.LAST_PROCESSED, Timestamp.now().getSeconds() * 1000)
                .set(VideoEntityProperties.HEIGHT, videoMetaData.getHeight())
                .set(VideoEntityProperties.WIDTH, videoMetaData.getWidth())
                .set(VideoEntityProperties.VIDEO_LAST_MODIFIED, videoLastModified)
                .set(VideoEntityProperties.EXISTS_IN_DRIVE, true)
                .set(VideoEntityProperties.DEDUPE_PROCESS, dedupeProcessed)
                .set(VideoEntityProperties.PHASHGEN_PROCESSED, phashgenProcessed)
                .set(VideoEntityProperties.VIDEO_SIZE, videoMetaData.getVideoSize())
                .set(VideoEntityProperties.NUM_KEYFRAMES, numKeyFrames)
                .build();
    }

    private long getVideoLastModified(VideoMetaData videoMetaData) {
        long videoLastModified;
        if (videoMetaData.getDateModified() != null)
            videoLastModified = videoMetaData.getDateModified().getValue();
        else
            videoLastModified = Timestamp.now().getSeconds() * 1000;
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
        boolean dedupeProcessed;
        boolean phashgenProcessed;
        long numberOfKeyFrames;
        if (videoMetaData.getVideoSize() > 500000000) {
            phashgenProcessed = true;
            dedupeProcessed = true;
            numberOfKeyFrames = -1;
        } else {
            phashgenProcessed = true;
            dedupeProcessed = false;
            numberOfKeyFrames = e.getLong(VideoEntityProperties.NUM_KEYFRAMES);
        }
        Entity task = Entity.newBuilder(e.getKey())
                .set(VideoEntityProperties.VIDEO_NAME, e.getString(VideoEntityProperties.VIDEO_NAME))
                .set(VideoEntityProperties.DURATION, e.getLong(VideoEntityProperties.DURATION))
                .set(VideoEntityProperties.LAST_PROCESSED, e.getLong(VideoEntityProperties.LAST_PROCESSED))
                .set(VideoEntityProperties.HEIGHT, e.getLong(VideoEntityProperties.HEIGHT))
                .set(VideoEntityProperties.WIDTH, e.getLong(VideoEntityProperties.WIDTH))
                .set(VideoEntityProperties.VIDEO_LAST_MODIFIED, videoLastModified)
                .set(VideoEntityProperties.EXISTS_IN_DRIVE, value)
                .set(VideoEntityProperties.DEDUPE_PROCESS, dedupeProcessed)
                .set(VideoEntityProperties.PHASHGEN_PROCESSED, phashgenProcessed)
                .set(VideoEntityProperties.VIDEO_SIZE, e.getLong(VideoEntityProperties.VIDEO_SIZE))
                .set(VideoEntityProperties.NUM_KEYFRAMES, numberOfKeyFrames)
                .build();
        datastore.put(task);
    }

    public void updatePropertyOfUser(UserStatus userStatus) {
        Datastore datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        Key key = createUserEntityKey(userStatus.getJobId(), userStatus.getEmail());
        Entity entity = findEntityOfUserTask(datastore, key);
        for (int i = 0; i < 3; i++) {
            resetUserProperty(entity, key, userStatus);
            Entity newEntity = datastore.get(key);
            if (newEntity.getBoolean(UserEntityProperties.PHASHGEN) == userStatus.isPhashgenProcessed() &&
                    newEntity.getBoolean(UserEntityProperties.DEDUPE) == userStatus.isDedupeProcessed() &&
                    newEntity.getLong(UserEntityProperties.FILTERED_VIDEOS_COUNT) == userStatus.getFilteredVideos() &&
                    newEntity.getLong(UserEntityProperties.TOTAL_VIDEOS) == userStatus.getTotalVideos()) {
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

    public void resetUserProperty(Entity e, Key key, UserStatus userStatus) {
        Entity task = Entity.newBuilder(key)
                .set(UserEntityProperties.USER_ID, e.getString(UserEntityProperties.USER_ID))
                .set(UserEntityProperties.NAME, e.getString(UserEntityProperties.NAME))
                .set(UserEntityProperties.EMAIL_ID, e.getString(UserEntityProperties.EMAIL_ID))
                .set(UserEntityProperties.TOTAL_VIDEOS, userStatus.getTotalVideos())
                .set(UserEntityProperties.FILTERED_VIDEOS_COUNT, userStatus.getFilteredVideos())
                .set(UserEntityProperties.PHASHGEN, userStatus.isPhashgenProcessed())
                .set(UserEntityProperties.DEDUPE, userStatus.isDedupeProcessed())
                .set(UserEntityProperties.CREATED, e.getTimestamp(UserEntityProperties.CREATED))
                .set(UserEntityProperties.DONE, e.getBoolean(UserEntityProperties.DONE))
                .build();
        datastore.put(task);
    }


    private Entity findEntityOfUserTask(Datastore datastore, Key key) {
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

    public ArrayList<String> getAllVideoIdsOfUser(String email) {
        ArrayList<String> videoIds = new ArrayList<>();
        logger.info("Retrieving videos of user=" + email);
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(email);
        Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                .setFilter(
                        StructuredQuery.CompositeFilter.and(
                                StructuredQuery.PropertyFilter.hasAncestor(ancestorPath),
                                StructuredQuery.PropertyFilter.eq(VideoEntityProperties.EXISTS_IN_DRIVE, true)))
                .build();
        QueryResults<Key> results = this.datastore.run(query);
        while (results.hasNext()) {
            videoIds.add(results.next().getName());
        }
        logger.info("Returning videoIds of user=" + email);
        return videoIds;
    }
}