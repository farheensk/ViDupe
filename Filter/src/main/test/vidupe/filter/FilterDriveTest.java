package vidupe.filter;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.Test;
import org.threeten.bp.Duration;
import vidupe.filter.constants.Constants;

import java.io.IOException;
import java.util.*;

public class FilterDriveTest {

    private List<String> keyList = new LinkedList<>();

    @Test
    public void doGetTest() {
    }

    @Test
    public void publishMessagesTest() {
        FilterDrive fd = new FilterDrive();
        Map<String,String> attributes= new HashMap<>();
        attributes.put("access_token","ya29.GluDBRxnF2uLis7aJQDYfAGGJAgUNR55rdg8RSpa3yNYjpzGp0hxa3nO-BHBeNN_pZvLl12GkDyDdAUkL_785XCeM3Bu42NIVpMv_InOseMwUKU2Z44pMGbrU-IP");
        attributes.put("client_id", getId());
        attributes.put("email", "gousiyafarheen3@gmail.com");
        attributes.put("ifExists","true");
        try {
            publishMessages(attributes);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    private String getId() {
        Random r = new Random();
        int k = r.nextInt(1000);
        String key = String.valueOf(k);
        keyList.add(key);
        return key;

    }
    void publishMessages(Map<String, String> attributes){
        String hostport = System.getenv("PUBSUB_EMULATOR_HOST");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext(true).build();
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
                        .setTotalTimeout(Duration.ofMinutes(5))
                        .setInitialRpcTimeout(Duration.ofSeconds(10))
                        .setMaxRpcTimeout(Duration.ofSeconds(11))
                        .setMaxRetryDelay(maxRetryDelay)
                        .build();


                TopicName topicName = TopicName.of(Constants.PROJECT, "filter-topic");

                Publisher publisher =
                        Publisher.newBuilder(topicName)
                                .setChannelProvider(channelProvider)
                                .setRetrySettings(retrySettings)
                                .setCredentialsProvider(credentialsProvider)
                                .build();
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().putAllAttributes(attributes).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);

            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            channel.shutdown();
        }
    }


}