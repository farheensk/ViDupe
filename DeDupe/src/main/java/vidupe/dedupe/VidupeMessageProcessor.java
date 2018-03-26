package vidupe.dedupe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.datastore.Key;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import vidupe.constants.Constants;
import vidupe.message.DeDupeMessage;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.apache.http.protocol.HTTP.UTF_8;

public class VidupeMessageProcessor implements MessageReceiver {

    private VidupeStoreManager vidupeStoreManager;

    public VidupeMessageProcessor(VidupeStoreManager vidupeStoreManager) {
        this.vidupeStoreManager = vidupeStoreManager;
    }

    private static long parseLong(String s, int base) {
        return new BigInteger(s, base).longValue();
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        ByteString messageFromFilter = message.getData();
        String messageString = messageFromFilter.toStringUtf8();
        ObjectMapper mapper = new ObjectMapper();
        DeDupeMessage deDupeMessage;
        try {
            deDupeMessage = mapper.readValue(messageString, DeDupeMessage.class);
            final List<VideoHashesInformation> videoHashesFromStore = retrieveVideoHashes(deDupeMessage);
            videoHashesFromStore.sort(new MapComparator("duration"));
            final List<List<VideoHashesInformation>> duplicates = groupDuplicateVideoFiles(videoHashesFromStore);
            DuplicateVideosList duplicateVideosList = DuplicateVideosList.builder()
                    .duplicateVideosList(duplicates).build();
            writeResultsToDataStore(deDupeMessage, duplicateVideosList);
            System.out.println(duplicates);
            for(int i=0;i<duplicates.size();i++){
                for (int j=0;j<duplicates.get(i).size();j++){
                    System.out.print(duplicates.get(i).get(j).videoID+"   , ");
                }
                System.out.println("\n ================ ");
            }

            vidupeStoreManager.changeExitsInDrivePropertyOfUser(deDupeMessage);
        } catch (Exception e) {
            e.printStackTrace();
        }

        consumer.ack();
    }

    private void writeResultsToDataStore(DeDupeMessage deDupeMessage, DuplicateVideosList duplicateVideosList) throws UnsupportedEncodingException {
        final String dataToFile = duplicateVideosList.toJsonString();
        ByteString data = ByteString.copyFromUtf8(dataToFile);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get("vidupe");
        Blob blob = bucket.create(deDupeMessage.getEmail()+"/"+deDupeMessage.getJobId(), dataToFile.getBytes(UTF_8), "application/json");
    }

    public ArrayList<Long> convertStringHashesToLong(ArrayList<String> hashes) {
        ArrayList<Long> longHashes = new ArrayList<>();
        for (String hash : hashes) {
            long longHash = parseLong(hash, 2);
            longHashes.add(longHash);
        }
        return longHashes;
    }

    private void publishMessage(DeDupeMessage deDupeMessage) throws Exception {
        TopicName topicName = TopicName.of(Constants.PROJECT, Constants.PHASHGEN_TOPIC);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();
            ByteString data = ByteString.copyFromUtf8(deDupeMessage.toJsonString());
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            messageIdFutures.add(messageIdFuture);

        } finally {
            // wait on any pending publish requests.
            List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

            for (String messageId : messageIds) {
                System.out.println("published with message ID: " + messageId);
            }

        }
    }

    private List<VideoHashesInformation> retrieveVideoHashes(DeDupeMessage deDupeMessage) {
        final Key[] videoIdsOfUser = vidupeStoreManager.getVideoIdsOfUser(deDupeMessage.getEmail());
        final List<VideoHashesInformation> videoHashesFromStore = vidupeStoreManager.getVideoHashesFromStore(videoIdsOfUser, deDupeMessage.getEmail());
        return videoHashesFromStore;
    }

    private List<List<VideoHashesInformation>> groupDuplicateVideoFiles(List<VideoHashesInformation> videoHashesFromStore) {
        int size = videoHashesFromStore.size();
        List<List<VideoHashesInformation>> duplicatesList = new ArrayList<>();
        int[] flag = new int[size];
        long smallDuration;
        for(int i = 0; i < size; i++){
            List<VideoHashesInformation> video1Duplicates = new ArrayList<>();
            if(flag[i] == 0){
                flag[i] =1;
                final VideoHashesInformation video1 = videoHashesFromStore.get(i);
                video1Duplicates.add(video1);
                smallDuration = video1.duration;
                for(int j = i + 1; j < size; j++){
                    final VideoHashesInformation video2 = videoHashesFromStore.get(j);
                    if((smallDuration - video2.duration)<=(smallDuration*0.1)){
                        smallDuration = (smallDuration<video2.duration)?smallDuration:video2.duration;
                        double distance = distanceBetweenVideos(video1, video2);
                        System.out.println(distance);
                        if(distance>=0.4){
                            flag[j] = 1;
                            video1Duplicates.add(video2);
                        }
                    }
                }
                if(video1Duplicates.size()>1)
                    duplicatesList.add(video1Duplicates);
            }
        }
        return duplicatesList;
    }

    public double distanceBetweenVideos(VideoHashesInformation video1, VideoHashesInformation video2) {
        final List<List<String>> hashesList1 = video1.getHashes();
        int N1 = hashesList1.size();
        final List<List<String>> hashesList2 = video2.getHashes();
        int N2 = hashesList2.size();
        int den = (N1 >= N2) ? N1 : N2;
        int C[][] = new int[N1 + 1][N2 + 1];
        for (int i = 1; i < N1 + 1; i++) {
            List<String> list1 = hashesList1.get(i - 1);
            for (int j = 1; j < N2 + 1; j++) {
                List<String> list2 = hashesList2.get(j - 1);
                int distance = computeDistance(list1, list2);
                if (distance <= 19)
                    C[i][j] = C[i - 1][j - 1] + 1;
                else
                    C[i][j] = ((C[i - 1][j] >= C[i][j - 1])) ? C[i - 1][j] : C[i][j - 1];
            }
        }
        double result = (double) (C[N1][N2]) / (double) (den);
        return result;
    }

    public int computeDistance(List<String> list1, List<String> list2) {
        ImagePhash imagePhash = new ImagePhash();
        int small = 64;
        for (String hash1 : list1) {
            for (String hash2 : list2) {
                int distance = imagePhash.hammingDistance(hash1, hash2);
                if (distance < small)
                    small = distance;
            }
        }
        return small;
    }

}