package vidupe.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
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

    private static VideoMetaData getMetaData(String id, String name, String description, DateTime dateModified, long size, long duration, long height, long width, String thumbnailLink) {
        return VideoMetaData.builder().id(id).name(name)
                .description(description)
                .dateModified(dateModified)
                .videoSize(size)
                .duration(duration)
                .height(height)
                .width(width)
                .thumbnailLink(thumbnailLink).build();
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        ByteString messageFromFilter = message.getData();
        String messageString = messageFromFilter.toStringUtf8();
        ObjectMapper mapper = new ObjectMapper();
        FilterMessage filterMessage;
        try {
            filterMessage = mapper.readValue(messageString, FilterMessage.class);
            List<VideoMetaData> listFiles = filter(filterMessage);
//        Long minHeight = Collections.min(listFiles, new MapComparator("height")).getHeight();
//        Long minWidth = Collections.min(listFiles, new MapComparator("width")).getWidth();
            String jobId = filterMessage.getJobId();
            String clientId = filterMessage.getEmail();
            int messageCount = 0;
            for (VideoMetaData videoMetaData : listFiles) {

                boolean proceedToHashGen = sendToHashGen(clientId, videoMetaData);

                if (proceedToHashGen) {
                    HashGenMessage hashGenMessage = HashGenMessage.builder().accessToken(filterMessage.getAccessToken())
                            .jobId(filterMessage.getJobId())
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
            logger.info("Number of videos to process: "+listFiles.size()+" , message_count "+messageCount);
            analyzeListFilesAndMessageCount(filterMessage, listFiles, clientId, messageCount);
            vidupeStoreManager.updatePropertyOfUsers(jobId,clientId, messageCount);
            vidupeStoreManager.deleteAllEntitiesIfNotExistsInDrive(clientId);
            consumer.ack();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void analyzeListFilesAndMessageCount(FilterMessage filterMessage, List<VideoMetaData> listFiles, String clientId, int messageCount) throws Exception {
        if(listFiles.size()>=2 && messageCount==0){
            DeDupeMessage deDupeMessage = DeDupeMessage.builder().email(clientId).jobId(filterMessage.getJobId()).build();
            publishMessageToDeDupe(deDupeMessage);
        }
        if(listFiles.size()==0 && messageCount == 0){
            writeResultsToDataStore(filterMessage, "{}");
        }
    }
    private void writeResultsToDataStore(FilterMessage filterMessage, String data) throws UnsupportedEncodingException {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get("vidupe");
        Blob blob = bucket.create(filterMessage.getEmail()+"/"+filterMessage.getJobId(), data.getBytes(UTF_8), "application/json");
    }

    private void publishMessageToDeDupe(DeDupeMessage deDupeMessage) throws Exception{
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

        }  catch(Exception e) {
            logger.error("An exception occurred when publishing message", e);
        }
        finally {
            // wait on any pending publish requests.
            List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

            for (String messageId : messageIds) {
                logger.info("Published message. messageId=" + messageId+", jobId="+deDupeMessage.getJobId());
            }

        }
    }

    private void publishMessage(HashGenMessage hashGenMessage) throws Exception{
        TopicName topicName = TopicName.of(Constants.PROJECT, Constants.FILTER_TOPIC);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();
            ByteString data = ByteString.copyFromUtf8(hashGenMessage.toJsonString());
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            messageIdFutures.add(messageIdFuture);
        }
         catch(Exception e) {
                logger.error("An exception occurred when publishing message", e);
            }
        finally {
                // wait on any pending publish requests.
                List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

                for (String messageId : messageIds) {
                    logger.info("Published message. messageId=" + messageId+", jobId="+hashGenMessage.getJobId());
                }

            }
    }

    boolean sendToHashGen(String clientId, VideoMetaData videoMetaData) {
        boolean proceedToHashGen = false;
        Entity entity = vidupeStoreManager.findByKey(videoMetaData.getId(), clientId);

        if(entity == null) {
            Entity entityCreated = vidupeStoreManager.createEntity(videoMetaData, clientId);
            proceedToHashGen = (entityCreated != null);
        }
       else {
            boolean shouldReplaceEntity = vidupeStoreManager.isModified(entity, videoMetaData);
            if(shouldReplaceEntity) {
                vidupeStoreManager.putEntity(videoMetaData, clientId);
                proceedToHashGen = true;
            } else {
                vidupeStoreManager.resetEntityProperty(entity, videoMetaData, true);
            }
        }
        return proceedToHashGen;
    }

    public List<VideoMetaData> filter(FilterMessage filterMessage) {
        final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE,
                DriveScopes.DRIVE_PHOTOS_READONLY,
                DriveScopes.DRIVE_FILE,
                DriveScopes.DRIVE_METADATA,
                DriveScopes.DRIVE_METADATA_READONLY,
                DriveScopes.DRIVE_APPDATA,
                DriveScopes.DRIVE_READONLY);
        List<VideoMetaData> filteredMetaData = null;
        try {
            String accessToken = filterMessage.getAccessToken();
            GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken).createScoped(SCOPES);

            Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
                    .setApplicationName("Duplicate video Detection").build();
            FileList result = drive.files().list().setFields(
                    "files(capabilities/canDownload,id,md5Checksum,mimeType,thumbnailLink,name,size,videoMediaMetadata,webContentLink)")
                    .execute();
         //   result = drive.files().list().execute();
            List<File> files = result.getFiles();
           // List<File> files = result.getFiles();

            List<VideoMetaData> metaDataList = new ArrayList<>();
            List<Long> height_list = new ArrayList<>();
            List<Long> width_list = new ArrayList<>();
            for (File file : files) {
                String type = file.getMimeType();
                if (Pattern.matches("video/.*", type)) {
                    String thumbnail_link = file.getThumbnailLink();
                    File.VideoMediaMetadata video_Media_MetaData = file.getVideoMediaMetadata();
                        metaDataList.add(getMetaData(file.getId(), file.getName(), file.getDescription(), file.getModifiedTime(), file.getSize(),
                                video_Media_MetaData.getDurationMillis(), video_Media_MetaData.getHeight(), video_Media_MetaData.getWidth(), thumbnail_link));

                        height_list.add(video_Media_MetaData.getHeight().longValue());
                        width_list.add(video_Media_MetaData.getWidth().longValue());
                    }
            }
            DurationFilter filter = new DurationFilter();
            filteredMetaData = filter.filterOutDurations(metaDataList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return filteredMetaData;
    }
}