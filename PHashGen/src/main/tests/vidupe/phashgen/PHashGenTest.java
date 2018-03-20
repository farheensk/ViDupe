package vidupe.phashgen;

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
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class PHashGenTest extends TestCase {

   public static void main(String args[]){
       VideoProcessor pHashGen = new VideoProcessor();
       pHashGen.extractKeyFrames("/media/farheen/01D26F1D020D3380/sample/","",new File("1920_1080.flv"));
        pHashGen.deleteFile("/media/farheen/01D26F1D020D3380/sample/","1920_1080.flv");
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