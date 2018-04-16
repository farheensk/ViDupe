package vidupe.dedupe;

import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.constants.UserEntityProperties;
import vidupe.constants.VideoEntityProperties;
import vidupe.message.DeDupeMessage;

import java.util.ArrayList;
import java.util.List;

public class VidupeStoreManager {

    private final Datastore datastore;
    final static int threshold = 21;

    private static final Logger logger = LoggerFactory.getLogger(VidupeStoreManager.class);

    public VidupeStoreManager(Datastore dataStore) {
        this.datastore = dataStore;
    }

    public Key[] getVideoIdsOfUser(String clientID) {
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(clientID);
        Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .build();
        QueryResults<Key> result = this.datastore.run(query);
        Key[] keys = Iterators.toArray(result, Key.class);
        return keys;
    }

    public List<VideoHashesInformation> getVideoAudioHashesFromStore(Key[] videoIdsOfUser, String clientId) {

        List<VideoHashesInformation> videoAudioHashes = new ArrayList<>();
        for (Key videoId : videoIdsOfUser) {
            VideoHashesInformation video1 = getSingleVideoAudioHashes(clientId, videoId);
            videoAudioHashes.add(video1);
        }
        return videoAudioHashes;

    }

    public VideoHashesInformation getSingleVideoAudioHashes(String clientId, Key videoId) {
        Entity[] videoEntities = getVideoHashEntities(clientId, videoId);
        List<String> hashes = enityToList(videoEntities);
        Entity e = retrieveVideoEntityInformation(videoId, clientId);
        Entity[] audioEntities = retrieveAudioEntityInformation(videoId, clientId);
        byte[] audioHashes = audioHashesFromBlob(audioEntities);
        List<List<String>> hashesAfterIntraComparison = intraComparison(hashes, threshold);
        VideoHashesInformation video = VideoHashesInformation.builder()
                .videoID(videoId.getName())
                .videoName(e.getString(VideoEntityProperties.VIDEO_NAME))
                .duration(e.getLong(VideoEntityProperties.DURATION))
                .numberOfKeyFrames(hashesAfterIntraComparison.size())
                .hashes(hashesAfterIntraComparison)
                .audioHashes(audioHashes)
                .build();
        return video;
    }

    public byte[] audioHashesFromBlob(Entity[] audioEntities) {
        byte[][] audioHashes = new byte[audioEntities.length][];
        int i = 0;
        int length = 0;
        for (Entity e : audioEntities) {
            Blob value = e.getBlob("value");
            byte[] byteArray = value.toByteArray();
            audioHashes[i] = new byte[byteArray.length];
            audioHashes[i] = value.toByteArray();
            i++;
            length = length + byteArray.length;
        }
        byte[] audioHashOfAVideo = new byte[length];
        int destinationPosition = 0;
        for (byte[] hash : audioHashes) {
            System.arraycopy(hash, 0, audioHashOfAVideo, destinationPosition, hash.length);
            destinationPosition = destinationPosition + hash.length;
        }

        return audioHashOfAVideo;
    }

    public Entity[] retrieveAudioEntityInformation(Key videoId, String clientId) {
        Key ancestorPath = datastore.newKeyFactory().setKind(clientId).newKey(videoId.getName());
        Query<Entity> query = Query.newEntityQueryBuilder().setKind("audio")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .setOrderBy(StructuredQuery.OrderBy.asc("__key__"))
                .build();
        QueryResults<Entity> result = this.datastore.run(query);
        return Iterators.toArray(result, Entity.class);
    }

    private Entity retrieveVideoEntityInformation(Key videoId, String clientId) {
        Key key = createKey(videoId.getName(), clientId);

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

    public Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("videos")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }

    public Entity[] getVideoHashEntities(String clientId, Key videoId) {
        Key ancestorPath = datastore.newKeyFactory().setKind(clientId).newKey(videoId.getName());
        Query<Entity> query = Query.newEntityQueryBuilder().setKind("VideoHashes")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .setOrderBy(StructuredQuery.OrderBy.asc("__key__"))
                .build();
        QueryResults<Entity> result = this.datastore.run(query);
        return Iterators.toArray(result, Entity.class);
    }

    public List<List<String>> intraComparison(List<String> videoHashesList, int threshold) {
        List<List<String>> groupedHashes = new ArrayList<>();
        if(videoHashesList!=null) {
            int size = videoHashesList.size();
            ComputeHammingDistance computeHammingDistance = new ComputeHammingDistance();
            int[] flag = new int[size];
            for (int i = 0; i < size; i++) {
                ArrayList<String> list = new ArrayList<>();
                if (flag[i] == 0) {
                    list.add(videoHashesList.get(i));
                    for (int j = i + 1; j < size; j++) {
                        double distance = computeHammingDistance.hammingDistance(videoHashesList.get(i), videoHashesList.get(j));
                        if (distance <= threshold) {
                            flag[j] = 1;
                            list.add(videoHashesList.get(j));
                        } else
                            break;
                    }
                    groupedHashes.add(list);
                }
            }
        }
        return groupedHashes;
    }

    private List<String> enityToList(Entity[] videoHashes) {
        List<String> hashes = new ArrayList<>();
        for (Entity e : videoHashes) {
            Long lhash = e.getLong("value");
            String hash = convertToStringHash(lhash);
            hashes.add(hash);
        }
        return hashes;
    }

    private String convertToStringHash(Long lhash) {
        String binaryString = Long.toBinaryString(lhash);
        String zeros = "0000000000000000000000000000000000000000000000000000000000000000"; //String of 64 zeros
        binaryString = zeros.substring(binaryString.length()) + binaryString;
        return binaryString;
    }

    public void changeExitsInDrivePropertyOfVideo(DeDupeMessage deDupeMessage) {
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(deDupeMessage.getEmail());
        Query<Entity> query = Query.newEntityQueryBuilder().setKind("videos")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .setOrderBy(StructuredQuery.OrderBy.asc("__key__"))
                .build();
        QueryResults<Entity> result = this.datastore.run(query);
        Entity[] entities = Iterators.toArray(result, Entity.class);
        for (Entity e : entities) {
            resetVideoEntityExistsInDriveProperty(e);
        }
    }

    public void resetVideoEntityExistsInDriveProperty(Entity e) {
        Entity task = Entity.newBuilder(e.getKey())
                .set(VideoEntityProperties.VIDEO_NAME, e.getString(VideoEntityProperties.VIDEO_NAME))
                .set(VideoEntityProperties.DURATION, e.getLong(VideoEntityProperties.DURATION))
                .set(VideoEntityProperties.LAST_PROCESSED, e.getLong(VideoEntityProperties.LAST_PROCESSED))
                .set(VideoEntityProperties.VIDEO_LAST_MODIFIED, e.getLong(VideoEntityProperties.VIDEO_LAST_MODIFIED))
                .set(VideoEntityProperties.EXISTS_IN_DRIVE, false)
                .set(VideoEntityProperties.DEDUPE_PROCESS, e.getBoolean(VideoEntityProperties.DEDUPE_PROCESS))
                .set(VideoEntityProperties.PHASHGEN_PROCESSED, e.getBoolean(VideoEntityProperties.PHASHGEN_PROCESSED))
                .set(VideoEntityProperties.VIDEO_SIZE, e.getLong(VideoEntityProperties.VIDEO_SIZE))
                .set(VideoEntityProperties.NUM_KEYFRAMES, e.getLong(VideoEntityProperties.NUM_KEYFRAMES))
                .build();
        for(int i=0;i<3;i++){
            datastore.put(task);
            Entity newEntity = datastore.get(e.getKey());
            if(newEntity.getBoolean(VideoEntityProperties.EXISTS_IN_DRIVE) == false){
                break;
            }
        }
    }

    public void resetUserEntityProperty(DeDupeMessage hashGenMessage, boolean doneDedupeProcess) {
        Key key = createUserEntityKey(hashGenMessage.getJobId(), hashGenMessage.getEmail());
        Entity entity = findEntityOfUserTask(datastore, key);
        Entity task = Entity.newBuilder(key)
                .set(UserEntityProperties.USER_ID, entity.getString(UserEntityProperties.USER_ID))
                .set(UserEntityProperties.NAME, entity.getString(UserEntityProperties.NAME))
                .set(UserEntityProperties.EMAIL_ID, entity.getString(UserEntityProperties.EMAIL_ID))
                .set(UserEntityProperties.TOTAL_VIDEOS, entity.getLong(UserEntityProperties.TOTAL_VIDEOS))
                .set(UserEntityProperties.FILTERED_VIDEOS_COUNT, entity.getLong(UserEntityProperties.FILTERED_VIDEOS_COUNT))
                .set(UserEntityProperties.PHASHGEN, entity.getBoolean(UserEntityProperties.PHASHGEN))
                .set(UserEntityProperties.DEDUPE, doneDedupeProcess)
                .set(UserEntityProperties.CREATED, entity.getTimestamp(UserEntityProperties.CREATED))
                .set(UserEntityProperties.DONE, entity.getBoolean(UserEntityProperties.DONE))
                .build();
        for(int i=0;i<3;i++){
            datastore.put(task);
            Entity newEntity = datastore.get(key);
            if(newEntity.getBoolean(UserEntityProperties.DEDUPE) == doneDedupeProcess){
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

    public boolean checkIfAllVideosAreProcessed(DeDupeMessage message) {
        String email = message.getEmail();
        logger.info("Checking if all videos are processed for user=" + email);

        boolean ifProcessed = false;
        for (int i = 0; i < 3; i++) {
            Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(email);
            Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                    .setFilter(
                            StructuredQuery.CompositeFilter.and(
                                    StructuredQuery.PropertyFilter.hasAncestor(ancestorPath),
                                    StructuredQuery.PropertyFilter.eq(VideoEntityProperties.DEDUPE_PROCESS, false)))
                    .build();
            QueryResults<Key> results = this.datastore.run(query);
            if (!results.hasNext()) {
                ifProcessed = true;
                logger.info("All videos are processed, user=" + email);
                break;
            }
        }
        logger.debug("Returning canDedupe=" + ifProcessed);
        return ifProcessed;
    }

    public void resetVideoEntityDeDupeProperty(DeDupeMessage deDupeMessage) {
        Key key = createKey(deDupeMessage.getVideoId(), deDupeMessage.getEmail());
        Entity e = datastore.get(key);
        Entity task = Entity.newBuilder(e.getKey())
                .set(VideoEntityProperties.VIDEO_NAME, e.getString(VideoEntityProperties.VIDEO_NAME))
                .set(VideoEntityProperties.DURATION, e.getLong(VideoEntityProperties.DURATION))
                .set(VideoEntityProperties.LAST_PROCESSED, e.getLong(VideoEntityProperties.LAST_PROCESSED))
                .set(VideoEntityProperties.VIDEO_LAST_MODIFIED, e.getLong(VideoEntityProperties.VIDEO_LAST_MODIFIED))
                .set(VideoEntityProperties.EXISTS_IN_DRIVE, e.getBoolean(VideoEntityProperties.EXISTS_IN_DRIVE))
                .set(VideoEntityProperties.DEDUPE_PROCESS, true)
                .set(VideoEntityProperties.PHASHGEN_PROCESSED, e.getBoolean(VideoEntityProperties.PHASHGEN_PROCESSED))
                .set(VideoEntityProperties.VIDEO_SIZE, e.getLong(VideoEntityProperties.VIDEO_SIZE))
                .set(VideoEntityProperties.NUM_KEYFRAMES, e.getLong(VideoEntityProperties.NUM_KEYFRAMES))
                .build();
        for(int i=0;i<3;i++){
            datastore.put(task);
            Entity newEntity = datastore.get(e.getKey());
            if(newEntity.getBoolean(VideoEntityProperties.DEDUPE_PROCESS) == true){
                break;
            }
        }
    }
}