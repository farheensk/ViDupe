package vidupe.phashgen;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.api.services.drive.Drive;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.Test;
import org.threeten.bp.Duration;
import vidupe.constants.Constants;
import vidupe.message.DeDupeMessage;
import vidupe.message.HashGenMessage;

import java.io.IOException;
import java.util.*;

import static junit.framework.TestCase.assertTrue;

public class VideoProcessorTest {
    String hashes = "1010110110011001011010010010000110111000010001000011000001011010$1011100010001001011011010010011001100001001100000001000011000011$1011100111000011001001000011100100110000100001000010000101100100$1110100110011000001010000010011001100100000000100010001101001100$1110110011001111001010000011000011100011100100110011000000110010$1011100110001110011010010011000100100100010011000000100100110000$1011100100111001011001110100110100111000100101100000010001001001$1010110000101001011001111001001010111000001001000010000101100001$1011100010011001011010011001001001101100011011000001001001100000$1010110100101100100010000001001101101100110011000000011001000000$";

    private List<String> keyList = new LinkedList<>();
    @Test
    public void generatePhash() {
        VideoProcessor videoProcessor = new VideoProcessor();
        String accessToken = "ya29.Glt7BanDIhHKwGBfkXzfYiiY6VabSDevaQIYYIDRd4YYnL5nUDCWZs27SuBRT1Ijrqv8A9schQ-Rx11JK3XxItvig3nTslQGuAMOxt2NHIWO7TS4vILZxWyoQ55i";
        HashGenMessage message = HashGenMessage.builder().accessToken(accessToken).videoId("0B-cyY07ful39VDBYQkxvZmRuek0")
                .videoName("video.mp4").email("").build();
        Drive drive = videoProcessor.getDrive(message);
        assertTrue("drive returned null", drive != null);
    }

    @Test
    public void processVideo() {
//        DeDupeMessage message1 = DeDupeMessage.builder().email("heenshaik9@gmail.com").build();
//        publishMessages(message1);
        VideoProcessor videoProcessor = new VideoProcessor();
        String accessToken = "accessToken\n" +
                " ya29.GluiBadP0DXWYuYqPCx18Bx3dQbSTcvpW4qnoeAtLoUubEIfOgOwpuY50XMd-bWxtmB-71s8ZSyR5CA_1DHY5da9SkPN1FsVOjgxxUpHWePuWw0d5XTnDucWrPDe";
        String videoId = "1agkI7o06HwMpOMsSvBLqn1MrrqQiouzu";
        //videoId = "123";
        HashGenMessage message = HashGenMessage.builder().accessToken(accessToken).videoId(videoId)
                .videoName("video.flv").email("").build();
        Drive drive = videoProcessor.getDrive(message);
        VideoAudioHashes hashes = videoProcessor.processVideo(message, drive);
        if(hashes!=null){
            System.out.println(hashes.getVideoHashes());
            System.out.println(hashes.getAudioHashes());
            assertTrue("hashes are null", hashes.getVideoHashes() != null);
        }
        else
            assertTrue(hashes == null);

    }

    @Test
    public void publishMessagesTest() {

    }
    private String getId() {
        Random r = new Random();
        int k = r.nextInt(1000);
        String key = String.valueOf(k);
        keyList.add(key);
        return key;

    }
    void publishMessages(DeDupeMessage message){
        String hostport = System.getenv("PUBSUB_EMULATOR_HOST");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext(true).build();
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();
        try {
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();


            // Set the channel and credentials provider when creating a `TopicAdminClient`.
            // Similarly for SubscriptionAdminClient
            try {
                Duration retryDelay = Duration.ofMillis(100); // default : 1 ms
                double retryDelayMultiplier = 2.0; // back off for repeated failures
                Duration maxRetryDelay = Duration.ofSeconds(5); // default : 10 seconds

                RetrySettings retrySettings = RetrySettings.newBuilder()
                        .setInitialRetryDelay(retryDelay)
                        .setRetryDelayMultiplier(retryDelayMultiplier)
                        .setMaxRetryDelay(maxRetryDelay)
                        .build();


                ProjectTopicName topicName = ProjectTopicName.of(Constants.PROJECT, "filter-topic");



                BatchingSettings batchSettings = BatchingSettings.newBuilder().setIsEnabled(false).build();
                Publisher publisher  = Publisher.newBuilder(topicName).setBatchingSettings(batchSettings).build();
                // publisher = Publisher.newBuilder(topicName).setRetrySettings(retrySettings).setBatchingSettings(batchSettings).build();
                ByteString data = ByteString.copyFromUtf8(message.toJsonString());
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                messageIdFutures.add(messageIdFuture);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            channel.shutdown();
        }
    }
}