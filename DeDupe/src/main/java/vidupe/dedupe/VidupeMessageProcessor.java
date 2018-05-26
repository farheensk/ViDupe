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
import lombok.extern.slf4j.Slf4j;
import vidupe.constants.Constants;
import vidupe.message.DeDupeMessage;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.http.protocol.HTTP.UTF_8;
import static vidupe.constants.Constants.*;

@Slf4j
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
        log.info("Received Message:" + message.getMessageId());
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
            //HashMap<String, String> bestVideoIds = getBestVideoIds(referenceVideo, duplicates);
            HashMap<String, String> bestVideoIds1 = getBestVideoResolution(referenceVideo, duplicates);
            DuplicateVideosList duplicateVideosList = DuplicateVideosList.builder()
                    .referenceVideo(referenceVideo)
                    .bestVideoIds(bestVideoIds1)
                    .duplicateVideosList(duplicates).build();
            writeResultsToDataStore(deDupeMessage, duplicateVideosList);
            vidupeStoreManager.resetVideoEntityDeDupeProperty(deDupeMessage);
            for (int i = 0; i < duplicates.size(); i++) {
                System.out.print(duplicates.get(i).videoID + "   , ");
            }
            System.out.println("\n ================ ");

            boolean ifProcessed = vidupeStoreManager.checkIfAllVideosAreProcessed(deDupeMessage);
            if (ifProcessed) {
                vidupeStoreManager.changeExitsInDrivePropertyOfVideo(deDupeMessage);
                vidupeStoreManager.resetUserEntityProperty(deDupeMessage, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        consumer.ack();
    }

//    private HashMap<String, String> getBestVideoIds(VideoHashesInformation referenceVideo, List<VideoHashesInformation> duplicates) {
//        long bestVideoSize = getBestVideoSize(referenceVideo, duplicates);
//        HashMap<String, String> bestVideoIds = findVideosWithBestVideoSize(bestVideoSize, referenceVideo, duplicates);
//        return bestVideoIds;
//    }

    private HashMap<String, String> getBestVideoResolution(VideoHashesInformation referenceVideo, List<VideoHashesInformation> duplicates) {
        HashMap<String, Long> bestVideoHeightWidth = getBestVideoHeightWidth(referenceVideo, duplicates);
        HashMap<String, String> bestVideoIds = findVideosWithBestVideoResolution(bestVideoHeightWidth, referenceVideo, duplicates);
        return bestVideoIds;
    }

//    private HashMap<String, String> findVideosWithBestVideoSize(long bestVideoSize, VideoHashesInformation referenceVideo, List<VideoHashesInformation> duplicates) {
//        HashMap<String, String> videoIds = new HashMap<>();
//        if (referenceVideo.getVideoSizeLong() == bestVideoSize) {
//            videoIds.put(referenceVideo.getVideoID(), referenceVideo.getVideoID());
//        }
//        for (VideoHashesInformation video : duplicates) {
//            if (video.getVideoSizeLong() == bestVideoSize) {
//                videoIds.put(video.getVideoID(), video.getVideoID());
//            }
//        }
//        return videoIds;
//    }

    private HashMap<String, String> findVideosWithBestVideoResolution(HashMap<String, Long> bestVideoResolution, VideoHashesInformation referenceVideo, List<VideoHashesInformation> duplicates) {
        HashMap<String, String> videoIds = new HashMap<>();
        if (referenceVideo.getVideoHeight() == bestVideoResolution.get("best-video-height")
                && referenceVideo.getVideoWidth() == bestVideoResolution.get("best-video-width")) {
            videoIds.put(referenceVideo.getVideoID(), referenceVideo.getVideoID());
        }
        for (VideoHashesInformation video : duplicates) {
            if (video.getVideoHeight() == bestVideoResolution.get("best-video-height")
                    && video.getVideoWidth() == bestVideoResolution.get("best-video-width")) {
                videoIds.put(video.getVideoID(), video.getVideoID());
            }
        }
        return videoIds;
    }

//    private long getBestVideoSize(VideoHashesInformation referenceVideo, List<VideoHashesInformation> duplicates) {
//        long referenceVideoSizeLong = referenceVideo.getVideoSizeLong();
//        long bestVideoSize = referenceVideoSizeLong;
//        for (VideoHashesInformation video : duplicates) {
//            if (video.getVideoSizeLong() > bestVideoSize) {
//                bestVideoSize = video.getVideoSizeLong();
//            }
//        }
//        return bestVideoSize;
//    }

    private HashMap<String, Long> getBestVideoHeightWidth(VideoHashesInformation referenceVideo, List<VideoHashesInformation> duplicates) {
        long bestVideoHeight = referenceVideo.getVideoHeight();
        long bestVideoWidth = referenceVideo.getVideoWidth();
        HashMap<String, Long> heightWidth = new HashMap<>();
        for (VideoHashesInformation video : duplicates) {
            if (video.getVideoHeight() > bestVideoHeight) {
                bestVideoHeight = video.getVideoHeight();
                bestVideoWidth = video.getVideoWidth();
            } else if (video.getVideoHeight() == bestVideoHeight) {
                bestVideoHeight = video.getVideoHeight();
                if (video.getVideoWidth() > bestVideoWidth) {
                    bestVideoWidth = video.getVideoWidth();
                }
            }
        }
        heightWidth.put("best-video-height", bestVideoHeight);
        heightWidth.put("best-video-width", bestVideoWidth);
        return heightWidth;
    }

    private void writeResultsToDataStore(DeDupeMessage deDupeMessage, DuplicateVideosList duplicateVideosList) throws UnsupportedEncodingException {
        log.info("Writing results to data store:Start");
        String dataToFile = duplicateVideosList.toJsonString();
        ByteString data = ByteString.copyFromUtf8(dataToFile);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get(Constants.BUCKET_NAME);
        Blob blob = bucket.create(deDupeMessage.getEmail() + "/" + deDupeMessage.getJobId() + "/" + deDupeMessage.getVideoId(), dataToFile.getBytes(UTF_8), "application/json");
        log.info("Writing results to data store:End");
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
        log.info("Retrieving hashes of user=" + deDupeMessage.getEmail() + ", videoId=" + deDupeMessage.getVideoId() + ":Start");
        Key[] videoIdsOfUser = vidupeStoreManager.getVideoIdsOfUser(deDupeMessage.getEmail());
        List<VideoHashesInformation> videoAudioHashesFromStore = vidupeStoreManager.getVideoAudioHashesFromStore(videoIdsOfUser, deDupeMessage.getEmail());
        log.info("Retrieving hashes of user=" + deDupeMessage.getEmail() + ", videoId=" + deDupeMessage.getVideoId() + ":End");
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
                    if (distance >= VIDEO_SIMILARITY)
                        if(similarity >= AUDIO_SIMILARITY) {
                            duplicatesList.add(video);
                        }
                        if((referenceVideo.getAudioHashes().length == 0)
                                && (video.getAudioHashes().length == 0)){
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