package vidupe.filter;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.Test;

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

                TopicName topicName = TopicName.of("winter-pivot-192220", "filter-topic");

                Publisher publisher =
                        Publisher.newBuilder(topicName)
                                .setChannelProvider(channelProvider)
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