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
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import vidupe.message.FilterMessage;
import vidupe.message.HashGenMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class VidupeMessageProcessor implements MessageReceiver {

    private VidupeStoreManager vidupeStoreManager;

    public VidupeMessageProcessor(VidupeStoreManager vidupeStoreManager) {
        this.vidupeStoreManager = vidupeStoreManager;
    }

    private static VideoMetaData getMetaData(String id, String name, String description, DateTime dateModified, long size, long duration, long height, long width) {
        return VideoMetaData.builder().id(id).name(name).description(description).dateModified(dateModified).videoSize(size).duration(duration).height(height).width(width).build();
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
            vidupeStoreManager.updatePropertyOfUsers(clientId, messageCount);
            vidupeStoreManager.deleteAllEntitiesIfNotExistsInDrive(clientId);
            consumer.ack();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void publishMessage(HashGenMessage hashGenMessage) throws Exception{
        TopicName topicName = TopicName.of("winter-pivot-192220", "filter-topic");
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();
            ByteString data = ByteString.copyFromUtf8(hashGenMessage.toJsonString());
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
        final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE);
        List<VideoMetaData> filteredMetaData = null;
        try {
            String accessToken = filterMessage.getAccessToken();
            GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken).createScoped(SCOPES);

            Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
                    .setApplicationName("Duplicate video Detection").build();
            FileList result = drive.files().list().setFields(
                    "files(capabilities/canDownload,id,md5Checksum,mimeType,name,size,videoMediaMetadata,webContentLink)")
                    .execute();
           // FileList result = drive.files().list().execute();
            List<File> files = result.getFiles();
           // List<File> files = result.getFiles();

            List<VideoMetaData> metaDataList = new ArrayList<>();
            List<Long> height_list = new ArrayList<>();
            List<Long> width_list = new ArrayList<>();
            for (File file : files) {
                String type = file.getMimeType();
                if (Pattern.matches("video/.*", type)) {

                    File.VideoMediaMetadata video_Media_MetaData = file.getVideoMediaMetadata();
                        metaDataList.add(getMetaData(file.getId(), file.getName(), file.getDescription(), file.getModifiedTime(), file.getSize(),
                                video_Media_MetaData.getDurationMillis(), video_Media_MetaData.getHeight(), video_Media_MetaData.getWidth()));

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