package vidupe.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.cloud.datastore.Entity;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import vidupe.filter.constants.Constants;
import vidupe.message.DeDupeMessage;
import vidupe.message.FilterMessage;
import vidupe.message.HashGenMessage;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.http.protocol.HTTP.UTF_8;

public class VidupeMessageProcessor implements MessageReceiver {

    private VidupeStoreManager vidupeStoreManager;
    private static final Logger logger = LoggerFactory.getLogger(VidupeMessageProcessor.class);

    public VidupeMessageProcessor(VidupeStoreManager vidupeStoreManager) {
        this.vidupeStoreManager = vidupeStoreManager;
    }

    private static VideoMetaData getMetaData(String id, String name, String description, DateTime dateModified, long size, long duration, long height, long width) {
        return VideoMetaData.builder().id(id).name(name)
                .description(description)
                .dateModified(dateModified)
                .videoSize(size)
                .duration(duration)
                .height(height)
                .width(width)
                .build();
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        ByteString messageFromFilter = message.getData();
        String messageString = messageFromFilter.toStringUtf8();
        ObjectMapper mapper = new ObjectMapper();
        FilterMessage filterMessage = null;
        int messageCount = 0;
        try {
            filterMessage = mapper.readValue(messageString, FilterMessage.class);
            List<VideoMetaData> metaDataList = getVideosList(filterMessage);
            DurationFilter durationFilter = new DurationFilter();
            List<VideoMetaData> listFiles = durationFilter.filterOutDurations(metaDataList);
            String jobId = filterMessage.getJobId();
            String clientId = filterMessage.getEmail();

            for (VideoMetaData videoMetaData : listFiles) {
                boolean proceedToHashGen = sendToHashGen(filterMessage, clientId, videoMetaData);
                if (proceedToHashGen) {
                    HashGenMessage hashGenMessage = HashGenMessage.builder().accessToken(filterMessage.getAccessToken())
                            .jobId(jobId)
                            .email(clientId)
                            .videoDuration(videoMetaData.getDuration())
                            .videoId(videoMetaData.getId())
                            .videoName(videoMetaData.getName())
                            .videoSize(videoMetaData.getVideoSize())
                            .build();
                    publishMessage(hashGenMessage);
                    messageCount++;
                }
            }
            logger.info("Number of videos to process: " + listFiles.size() + " , message_count " + messageCount);
            vidupeStoreManager.deleteAllEntitiesIfNotExistsInDrive(clientId);
            analyzeListFilesAndMessageCount(filterMessage, listFiles, messageCount, metaDataList.size());
            consumer.ack();
        } catch (Exception e) {
            String path = filterMessage.getEmail() + "/" + filterMessage.getJobId();
            try {
                writeResultsToDataStore(path, "401");
            } catch (UnsupportedEncodingException e1) {
                e1.printStackTrace();
            }
            updateUserStatus(filterMessage, messageCount, 0, true, true);
            e.printStackTrace();
        }
    }

    private void analyzeListFilesAndMessageCount(FilterMessage filterMessage, List<VideoMetaData> listFiles, int messageCount, int totalVideos) throws Exception {
        if (messageCount > 0) {
            updateUserStatus(filterMessage, messageCount, totalVideos, false, false);
        }
        if (messageCount == 0) {
            if (listFiles.size() >= 2) {
                updateUserStatus(filterMessage, messageCount, totalVideos, true, false);
                ArrayList<String> videoIdsOfUser = vidupeStoreManager.getAllVideoIdsOfUser(filterMessage.getEmail());
                for (String videoId : videoIdsOfUser) {
                    DeDupeMessage messageToDeDupe = DeDupeMessage.builder().jobId(filterMessage.getJobId())
                            .email(filterMessage.getEmail()).videoId(videoId).build();
                    publishMessageToDeDupe(messageToDeDupe);
                }
            }
            if (listFiles.size() == 0) {
                String path = filterMessage.getEmail() + "/" + filterMessage.getJobId();
                writeResultsToDataStore(path, "{}");
                updateUserStatus(filterMessage, messageCount, totalVideos, true, true);
            }
        }
    }

    public void updateUserStatus(FilterMessage filterMessage, int messageCount, int totalVideos, boolean ifPhashgenProcessed, boolean ifDedupeProcessed) {

        UserStatus userStatus = UserStatus.builder()
                .jobId(filterMessage.getJobId())
                .email(filterMessage.getEmail())
                .phashgenProcessed(ifPhashgenProcessed)
                .dedupeProcessed(ifDedupeProcessed)
                .totalVideos(totalVideos)
                .filteredVideos(messageCount)
                .build();
        vidupeStoreManager.updatePropertyOfUser(userStatus);
    }

    private void writeResultsToDataStore(String path, String data) throws UnsupportedEncodingException {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get(Constants.BUCKET_NAME);
        Blob blob = bucket.create(path, data.getBytes(UTF_8), "application/json");
    }

    private void publishMessageToDeDupe(DeDupeMessage deDupeMessage) throws Exception {
        TopicName topicName = TopicName.of(Constants.PROJECT, Constants.PHASHGEN_TOPIC);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            Duration retryDelay = Duration.ofMillis(100); // default : 1 ms
            double retryDelayMultiplier = 2.0; // back off for repeated failures
            Duration maxRetryDelay = Duration.ofSeconds(2); // default : 10 seconds

            RetrySettings retrySettings = RetrySettings.newBuilder()
                    .setInitialRetryDelay(retryDelay)
                    .setRetryDelayMultiplier(retryDelayMultiplier)
                    .setTotalTimeout(Duration.ofMinutes(20))
                    .setInitialRpcTimeout(Duration.ofSeconds(10))
                    .setMaxRpcTimeout(Duration.ofSeconds(11))
                    .setMaxRetryDelay(maxRetryDelay)
                    .build();



//            publisher =
//                    Publisher.newBuilder(topicName)
//                            .setRetrySettings(retrySettings)
//                            .build();
            // Create a publisher instance with default settings bound to the topic
            BatchingSettings batchSettings = BatchingSettings.newBuilder().setIsEnabled(false).build();
            publisher = Publisher.newBuilder(topicName).setBatchingSettings(batchSettings)
                    .setRetrySettings(retrySettings).build();
            ByteString data = ByteString.copyFromUtf8(deDupeMessage.toJsonString());
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            messageIdFutures.add(messageIdFuture);
        } catch (Exception e) {
            logger.error("An exception occurred when publishing message", e);
        } finally {
            // wait on any pending publish requests.
            List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

            for (String messageId : messageIds) {
                logger.info("Published message. messageId=" + messageId + ", jobId=" + deDupeMessage.getJobId());
            }
        }
    }

    private void publishMessage(HashGenMessage hashGenMessage) throws Exception {
        TopicName topicName = TopicName.of(Constants.PROJECT, Constants.FILTER_TOPIC);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();
        try {
            Duration retryDelay = Duration.ofMillis(100); // default : 1 ms
            double retryDelayMultiplier = 2.0; // back off for repeated failures
            Duration maxRetryDelay = Duration.ofSeconds(2); // default : 10 seconds

            RetrySettings retrySettings = RetrySettings.newBuilder()
                    .setInitialRetryDelay(retryDelay)
                    .setRetryDelayMultiplier(retryDelayMultiplier)
                    .setTotalTimeout(Duration.ofMinutes(20))
                    .setInitialRpcTimeout(Duration.ofSeconds(10))
                    .setMaxRpcTimeout(Duration.ofSeconds(11))
                    .setMaxRetryDelay(maxRetryDelay)
                    .build();
            // Create a publisher instance with default settings bound to the topic
            BatchingSettings batchSettings = BatchingSettings.newBuilder().setIsEnabled(false).build();
            publisher = Publisher.newBuilder(topicName).setBatchingSettings(batchSettings)
                    .setRetrySettings(retrySettings).build();
            ByteString data = ByteString.copyFromUtf8(hashGenMessage.toJsonString());
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            messageIdFutures.add(messageIdFuture);


        } catch (Exception e) {
            logger.error("An exception occurred when publishing message", e);
        } finally {
            // wait on any pending publish requests.
            List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();
            for (String messageId : messageIds) {
                logger.info("Published message. messageId=" + messageId + ", jobId=" + hashGenMessage.getJobId());
            }
        }
    }

    boolean sendToHashGen(FilterMessage filterMessage, String clientId, VideoMetaData videoMetaData) throws UnsupportedEncodingException {
        boolean proceedToHashGen = false;
        Entity entity = vidupeStoreManager.findByKey(videoMetaData.getId(), clientId);
        if (videoMetaData.getVideoSize() > 500000000) {
            if (entity == null) {
                Entity entityCreated = vidupeStoreManager.createEntity(videoMetaData, clientId);
                proceedToHashGen = false;
            } else {
                boolean shouldReplaceEntity = vidupeStoreManager.isModified(entity, videoMetaData);
                if (shouldReplaceEntity) {
                    vidupeStoreManager.putEntity(videoMetaData, clientId);
                    proceedToHashGen = false;
                } else {
                    vidupeStoreManager.resetEntityProperty(entity, videoMetaData, true);
                }
            }
            String path = filterMessage.getEmail() + "/" + filterMessage.getJobId() + "/" + videoMetaData.getId();
            writeResultsToDataStore(path, "{}");
        } else {
            if (entity == null) {
                Entity entityCreated = vidupeStoreManager.createEntity(videoMetaData, clientId);
                proceedToHashGen = (entityCreated != null);
            } else {
                boolean shouldReplaceEntity = vidupeStoreManager.isModified(entity, videoMetaData);
                if (shouldReplaceEntity) {
                    vidupeStoreManager.putEntity(videoMetaData, clientId);
                    proceedToHashGen = true;
                } else {
                    vidupeStoreManager.resetEntityProperty(entity, videoMetaData, true);
                }
            }
        }
        return proceedToHashGen;
    }

    public List<VideoMetaData> getVideosList(FilterMessage filterMessage) {
        final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE);
        List<VideoMetaData> videoMetaDataList = new ArrayList<>();
        try {
            String accessToken = filterMessage.getAccessToken();
            GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken).createScoped(SCOPES);
            Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
                    .setApplicationName("Duplicate video Detection").build();
            FileList result = drive.files().list().setFields(
                    "files(capabilities/canDownload,id,md5Checksum,mimeType,name,size,videoMediaMetadata,webContentLink)")
                    .execute();
            List<File> files = result.getFiles();
//            List<VideoMetaData> metaDataList = new ArrayList<>();
            for (File file : files) {
                String type = file.getMimeType();
                if (Pattern.matches("video/.*", type)) {
                    File.VideoMediaMetadata video_Media_MetaData = file.getVideoMediaMetadata();
                    if (video_Media_MetaData != null) {
                        videoMetaDataList.add(getMetaData(file.getId(), file.getName(), file.getDescription(), file.getModifiedTime(), file.getSize(),
                                video_Media_MetaData.getDurationMillis(), video_Media_MetaData.getHeight(), video_Media_MetaData.getWidth()));

                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return videoMetaDataList;
    }
}