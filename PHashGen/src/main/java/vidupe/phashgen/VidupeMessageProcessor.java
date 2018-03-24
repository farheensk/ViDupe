package vidupe.phashgen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.services.drive.Drive;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.constants.Constants;
import vidupe.message.DeDupeMessage;
import vidupe.message.HashGenMessage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class VidupeMessageProcessor implements MessageReceiver {

    private VidupeStoreManager vidupeStoreManager;
    private static final Logger logger = LoggerFactory.getLogger(VidupeMessageProcessor.class);

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
            final Drive drive = videoProcessor.getDrive(hashGenMessage);
            ArrayList<String> hashes = videoProcessor.processVideo(hashGenMessage,drive);
            final ArrayList<Long> longHashes = convertStringHashesToLong(hashes);

            final boolean canSendMessage = vidupeStoreManager.writeInDataStore(longHashes, hashGenMessage);
            if(canSendMessage){
                DeDupeMessage messageToDeDupe = DeDupeMessage.builder().jobId(hashGenMessage.getJobId()).email(hashGenMessage.getEmail()).build();
                publishMessage(messageToDeDupe);
            }
        } catch (Exception e) {
            logger.error("Message receive exception: ", e);
        }

        consumer.ack();
    }

    public ArrayList<Long> convertStringHashesToLong(ArrayList<String> hashes) {
        ArrayList<Long> longHashes = new ArrayList<>();
        for(String hash: hashes){
            long longHash = parseLong(hash, 2);
            longHashes.add(longHash);
        }
        return longHashes;
    }

    private void publishMessage(DeDupeMessage deDupeMessage) throws Exception{
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
                logger.info("Published message. messageId=" + messageId+", jobId="+deDupeMessage.getJobId());
            }

        }
    }

}