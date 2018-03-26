package vidupe.dedupe;

import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import vidupe.constants.UserEntityProperties;
import vidupe.constants.VideoEntityProperties;
import vidupe.message.DeDupeMessage;

import java.util.ArrayList;
import java.util.List;

public class VidupeStoreManager {

    private final Datastore datastore;

    public VidupeStoreManager(Datastore dataStore) {
        this.datastore = dataStore;
    }

    public List<Long> retrieveHashes(){

        return null;
    }

    public Key[] retrieveEntityInformation() {
        Query<Key> query = Query.newKeyQueryBuilder().setKind("users")
                .setFilter(StructuredQuery.PropertyFilter.gt(UserEntityProperties.TOTAL_VIDEOS, 1))
                .setFilter(StructuredQuery.PropertyFilter.gt(UserEntityProperties.VIDEOS_PROCESSED, 1))
                .setFilter(StructuredQuery.PropertyFilter.eq(UserEntityProperties.VIDEOS_PROCESSED, UserEntityProperties.TOTAL_VIDEOS))
                .build();
        QueryResults<Key> result = this.datastore.run(query);
        final Key[] keys = Iterators.toArray(result, Key.class);
        return keys;
    }

    public Key[] getVideoIdsOfUser(String clientID) {
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(clientID);
        Query<Key> query = Query.newKeyQueryBuilder().setKind("videos")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .build();
        QueryResults<Key> result = this.datastore.run(query);
        final Key[] keys = Iterators.toArray(result, Key.class);
        return keys;
    }

    public List<VideoHashesInformation> getVideoHashesFromStore(Key[] videoIdsOfUser, String clientId) {

        List<VideoHashesInformation> videoHashes = new ArrayList<>();
        for(Key videoId: videoIdsOfUser){
            final Entity[] videoEntities = getVideoHashEntities(clientId, videoId);
            List<String> hashes = enityToList(videoEntities);
            Entity e = retrieveVideoEntityInformation(videoId, clientId);
            List<List<String>> hashesAfterIntraComparison = intraComparison(hashes, 19);
            VideoHashesInformation video1 = VideoHashesInformation.builder()
                    .videoID(videoId.getName())
                    .videoName(e.getString(VideoEntityProperties.VIDEO_NAME))
                    .duration(e.getLong(VideoEntityProperties.DURATION))
                    .thumbnailLink(e.getString(VideoEntityProperties.THUMBNAIL_LINK))
                    .hashes(hashesAfterIntraComparison).build();
            videoHashes.add(video1);
        }
        return videoHashes;

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

    private Entity[] getVideoHashEntities(String clientId, Key videoId) {
        Key ancestorPath = datastore.newKeyFactory().setKind(clientId).newKey(videoId.getName());
        Query<Entity> query = Query.newEntityQueryBuilder().setKind("hashes")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .setOrderBy(StructuredQuery.OrderBy.asc("__key__"))
                .build();
        QueryResults<Entity> result = this.datastore.run(query);
        return Iterators.toArray(result, Entity.class);
    }

    public List<List<String>> intraComparison(List<String> videoHashesList, int threshold) {
        final int size = videoHashesList.size();
        ImagePhash imagePhash = new ImagePhash();
        List<List<String>> groupedHashes = new ArrayList<>();
        int[] flag = new int[size];
        for (int i = 0; i < size; i++) {
            ArrayList<String> list = new ArrayList<>();
            if (flag[i] == 0) {
                list.add(videoHashesList.get(i));
                for (int j = i + 1; j < size; j++) {
                    double distance = imagePhash.hammingDistance(videoHashesList.get(i), videoHashesList.get(j));
                    if (distance <= threshold) {
                        flag[j] = 1;
                        list.add(videoHashesList.get(j));
                    } else
                        break;
                }
                groupedHashes.add(list);
            }
        }
        return groupedHashes;
    }

    private List<String> enityToList(Entity[] videoHashes) {
        List<String> hashes = new ArrayList<>();
        for(Entity e: videoHashes){
            Long lhash = e.getLong("value");
            String hash = convertToStringHash(lhash);
            hashes.add(hash);
        }
        return hashes;
    }

    private String convertToStringHash(Long lhash) {
        String binaryString = Long.toBinaryString(lhash);
        String zeros = "0000000000000000000000000000000000000000000000000000000000000000"; //String of 64 zeros
        binaryString = zeros.substring(binaryString.length())+ binaryString;
        return binaryString;
    }

    public void changeExitsInDrivePropertyOfUser(DeDupeMessage deDupeMessage) {
        Key ancestorPath = datastore.newKeyFactory().setKind("user").newKey(deDupeMessage.getEmail());
        Query<Entity> query = Query.newEntityQueryBuilder().setKind("videos")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorPath))
                .setOrderBy(StructuredQuery.OrderBy.asc("__key__"))
                .build();
        QueryResults<Entity> result = this.datastore.run(query);
        Entity[] entities = Iterators.toArray(result, Entity.class);
        for(Entity e:entities){
            resetEntityProperty(e);
        }
    }
    public void resetEntityProperty(Entity e) {
        Entity task = Entity.newBuilder(e.getKey())
                .set(VideoEntityProperties.VIDEO_NAME, e.getString(VideoEntityProperties.VIDEO_NAME))
                .set(VideoEntityProperties.DURATION, e.getLong(VideoEntityProperties.DURATION))
                .set(VideoEntityProperties.LAST_PROCESSED, e.getLong(VideoEntityProperties.LAST_PROCESSED))
                .set(VideoEntityProperties.VIDEO_LAST_MODIFIED, e.getLong(VideoEntityProperties.VIDEO_LAST_MODIFIED))
                .set(VideoEntityProperties.EXISTS_IN_DRIVE, false)
                .set(VideoEntityProperties.PROCESSED, e.getBoolean(VideoEntityProperties.PROCESSED))
                .set(VideoEntityProperties.THUMBNAIL_LINK, e.getString(VideoEntityProperties.THUMBNAIL_LINK))
                .build();
        datastore.put(task);
    }
}