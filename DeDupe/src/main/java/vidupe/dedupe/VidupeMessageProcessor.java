package vidupe.dedupe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.datastore.Key;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.musicg.fingerprint.FingerprintSimilarity;
import com.musicg.fingerprint.FingerprintSimilarityComputer;
import vidupe.message.DeDupeMessage;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.apache.http.protocol.HTTP.UTF_8;
import static vidupe.constants.Constants.*;

public class VidupeMessageProcessor implements MessageReceiver {

    private VidupeStoreManager vidupeStoreManager;
    //  final static int THRESHOLD = 21;


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
            List<VideoHashesInformation> videoAudioHashes = retrieveHashes(deDupeMessage);
            Key videoId = vidupeStoreManager.createKey(deDupeMessage.getVideoId(), deDupeMessage.getEmail());
            VideoHashesInformation referenceVideo = vidupeStoreManager.getSingleVideoAudioHashes(deDupeMessage.getEmail(), videoId);
            DurationFilter filter = new DurationFilter();
            filter.filterOutDurations(videoAudioHashes);
            List<VideoHashesInformation> duplicates = groupDuplicateVideoFiles(videoAudioHashes, referenceVideo);
            DuplicateVideosList duplicateVideosList = DuplicateVideosList.builder()
                    .referenceVideo(referenceVideo).duplicateVideosList(duplicates).build();
            writeResultsToDataStore(deDupeMessage, duplicateVideosList);
            vidupeStoreManager.resetVideoEntityDeDupeProperty(deDupeMessage);
            for (int i = 0; i < duplicates.size(); i++) {
                System.out.print(duplicates.get(i).videoID + "   , ");
            }
            System.out.println("\n ================ ");

            boolean ifProcessed = vidupeStoreManager.checkIfAllVideosAreProcessed(deDupeMessage);
            if(ifProcessed){
                vidupeStoreManager.changeExitsInDrivePropertyOfVideo(deDupeMessage);
                vidupeStoreManager.resetUserEntityProperty(deDupeMessage, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        consumer.ack();
    }

    private void writeResultsToDataStore(DeDupeMessage deDupeMessage, DuplicateVideosList duplicateVideosList) throws UnsupportedEncodingException {
        String dataToFile = duplicateVideosList.toJsonString();
        ByteString data = ByteString.copyFromUtf8(dataToFile);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get("vidupe");
        Blob blob = bucket.create(deDupeMessage.getEmail() + "/" + deDupeMessage.getJobId() + "/" + deDupeMessage.getVideoId(), dataToFile.getBytes(UTF_8), "application/json");
    }

    public ArrayList<Long> convertStringHashesToLong(ArrayList<String> hashes) {
        ArrayList<Long> longHashes = new ArrayList<>();
        for (String hash : hashes) {
            long longHash = parseLong(hash, 2);
            longHashes.add(longHash);
        }
        return longHashes;
    }

    List<VideoHashesInformation> retrieveHashes(DeDupeMessage deDupeMessage) {
        Key[] videoIdsOfUser = vidupeStoreManager.getVideoIdsOfUser(deDupeMessage.getEmail());
        List<VideoHashesInformation> videoAudioHashesFromStore = vidupeStoreManager.getVideoAudioHashesFromStore(videoIdsOfUser, deDupeMessage.getEmail());
        return videoAudioHashesFromStore;
    }

    private List<VideoHashesInformation> groupDuplicateVideoFiles(List<VideoHashesInformation> videoHashesFromStore, VideoHashesInformation referenceVideo) {
        int size = videoHashesFromStore.size();
        List<VideoHashesInformation> duplicatesList = new ArrayList<>();
        long referenceVideoDuration = referenceVideo.getDuration();
        int[] flag = new int[size];
        for (VideoHashesInformation video : videoHashesFromStore) {
            if (!(referenceVideo.getVideoID().equals(video.getVideoID()))) {
                if (Math.abs(referenceVideoDuration - video.getDuration()) <= referenceVideoDuration * 0.01) {
                    double distance = distanceBetweenVideos(referenceVideo, video);
                    float similarity = compareAudioHashes(referenceVideo.getAudioHashes(), video.getAudioHashes());
                    System.out.println(referenceVideo.getVideoName() + " " + video.getVideoName() + " Video Similarity: " + distance);
                    System.out.println(referenceVideo.getVideoName() + " " + video.getVideoName() + ", Audio similarity: " + similarity);
                    if (distance >= VIDEO_SIMILARITY && similarity >= AUDIO_SIMILARITY) {
                        duplicatesList.add(video);
                    }
                }
            }
        }
        return duplicatesList;
    }

    public float compareAudioHashes(byte[] audio1, byte[] audio2) {
        FingerprintSimilarity fingerprintSimilarity = new FingerprintSimilarityComputer(audio1, audio2).getFingerprintsSimilarity();
        return fingerprintSimilarity.getSimilarity();
    }

    public double distanceBetweenVideos(VideoHashesInformation video1, VideoHashesInformation video2) {
        List<List<String>> hashesList1 = video1.getHashes();
        int N1 = hashesList1.size();
        List<List<String>> hashesList2 = video2.getHashes();
        int N2 = hashesList2.size();
        //int den = N1;
        int den = (N1 >= N2) ? N1 : N2;
        int C[][] = new int[N1 + 1][N2 + 1];
        for (int i = 1; i < N1 + 1; i++) {
            List<String> list1 = hashesList1.get(i - 1);
            for (int j = 1; j < N2 + 1; j++) {
                List<String> list2 = hashesList2.get(j - 1);
                int distance = computeDistance(list1, list2);
                if (distance <= THRESHOLD)
                    C[i][j] = C[i - 1][j - 1] + 1;
                else
                    C[i][j] = ((C[i - 1][j] >= C[i][j - 1])) ? C[i - 1][j] : C[i][j - 1];
            }
        }
        double result = (double) (C[N1][N2]) / (double) (den);
        return result;
    }

    public int computeDistance(List<String> list1, List<String> list2) {
        ComputeHammingDistance computeHammingDistance = new ComputeHammingDistance();
        int small = 64;
        for (String hash1 : list1) {
            for (String hash2 : list2) {
                int distance = computeHammingDistance.hammingDistance(hash1, hash2);
                if (distance < small)
                    small = distance;
            }
        }
        return small;
    }

}