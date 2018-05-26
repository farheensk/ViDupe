package vidupe.phashgen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.drive.Drive;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.threeten.bp.Duration;
import vidupe.constants.Constants;
import vidupe.message.DeDupeMessage;
import vidupe.message.HashGenMessage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

@Slf4j
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
        HashGenMessage hashGenMessage;
        try {
            hashGenMessage = mapper.readValue(messageString, HashGenMessage.class);
            VideoProcessor videoProcessor = new VideoProcessor();
            Drive drive = videoProcessor.getDrive(hashGenMessage);
            VideoAudioHashes videoAudioHashes = videoProcessor.processVideo(hashGenMessage, drive);
            if(videoAudioHashes!=null){
                ArrayList<Long> longHashes = convertStringHashesToLong(videoAudioHashes.getVideoHashes());

                boolean canSendMessage = vidupeStoreManager.writeInDataStore(longHashes, videoAudioHashes.getAudioHashes(), hashGenMessage);
                if (canSendMessage) {
                    boolean doneHashgenProcess = true;
                    ArrayList<String> videoIdsOfUser = vidupeStoreManager.getAllVideoIdsOfUser(hashGenMessage.getEmail());
                    for (String videoId : videoIdsOfUser) {
                        DeDupeMessage messageToDeDupe = DeDupeMessage.builder().jobId(hashGenMessage.getJobId())
                                .email(hashGenMessage.getEmail()).videoId(videoId).build();
                        publishMessage(messageToDeDupe);
                    }
                    vidupeStoreManager.resetUserEntityProperty(hashGenMessage, doneHashgenProcess);
                }
            }
            consumer.ack();
        } catch (Exception e) {
            log.info("Message receive exception: ", e);
            //consumer.nack();
        }

    }

    public ArrayList<Long> convertStringHashesToLong(ArrayList<String> hashes) {
        ArrayList<Long> longHashes = new ArrayList<>();
        if (hashes != null) {
            for (String hash : hashes) {
                long longHash = parseLong(hash, 2);
                longHashes.add(longHash);
            }
        }
        return longHashes;
    }

    private void publishMessage(DeDupeMessage deDupeMessage) throws Exception {
        ProjectTopicName topicName = ProjectTopicName.of(Constants.PROJECT, Constants.PHASHGEN_TOPIC);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();
        try {
            // Retry settings control how the publisher handles retryable failures
//            Duration retryDelay = Duration.ofMillis(100); // default : 1 ms
//            double retryDelayMultiplier = 2.0; // back off for repeated failures
//            Duration maxRetryDelay = Duration.ofSeconds(5); // default : 10 seconds
//
//            RetrySettings retrySettings = RetrySettings.newBuilder()
//                    .setInitialRetryDelay(retryDelay)
//                    .setRetryDelayMultiplier(retryDelayMultiplier)
//                    .setMaxRetryDelay(maxRetryDelay)
//                    .build();

                      // Create a publisher instance with default settings bound to the topic
            BatchingSettings batchSettings = BatchingSettings.newBuilder().setIsEnabled(false).build();
            publisher = Publisher.newBuilder(topicName).setBatchingSettings(batchSettings).build();
           // publisher = Publisher.newBuilder(topicName).setRetrySettings(retrySettings).setBatchingSettings(batchSettings).build();
            ByteString data = ByteString.copyFromUtf8(deDupeMessage.toJsonString());
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            messageIdFutures.add(messageIdFuture);

        } catch (Exception e) {
            log.error("An exception occurred when publishing message", e);
        } finally {
            // wait on any pending publish requests.
            List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

            for (String messageId : messageIds) {
                log.info("Published message. messageId=" + messageId + ", jobId=" + deDupeMessage.getJobId());
            }
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
            }
        }
    }

}