package vidupe.filter;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@WebServlet("/filter")
public class FilterDrive extends HttpServlet {
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        try {

            StringBuilder display = receiveMessages();
//            List<VideoMetaData> listFiles= filter(attributesReceived);
//
//            StringBuilder display = new StringBuilder("default");
//            for(VideoMetaData videoMetaData:listFiles){
//                display.append(videoMetaData.getName());
//                display.append(videoMetaData.getDuration());
//                display.append(videoMetaData.getId());
//                display.append(videoMetaData.getVideoSize());
//            }
//
            response.getWriter().print(display);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<VideoMetaData>  filter(Map<String,String> attributesReceived){
        List<VideoMetaData>  filteredMetaData = null;
        try{
            String accessToken = attributesReceived.get("access_token");
            URL url = new URL("https://www.googleapis.com/oauth2/v1/userinfo?access_token=" + accessToken);
            URLConnection conn = url.openConnection();
            GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken);

            Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
                    .setApplicationName("Duplicate video Detection").build();
            FileList result = drive.files().list().setFields(
                    "files(capabilities/canDownload,id,md5Checksum,mimeType,name,size,videoMediaMetadata,webContentLink)")
                    .execute();
            List<File> files = result.getFiles();

            List<VideoMetaData> metaDataList = new ArrayList<>();
            List<Long> height_list = new ArrayList<>();
            List<Long> width_list = new ArrayList<>();
            for (File file : files) {
                String type = file.getMimeType();
                if(Pattern.matches("video/\\D*", type)){


                    VideoMetaDataBuilder builder = new VideoMetaDataBuilder();
                    File.VideoMediaMetadata video_Media_MetaData = file.getVideoMediaMetadata();

                    metaDataList.add(getMetaData(builder, file.getId(), file.getName(),file.getDescription(), file.getSize(),
                            video_Media_MetaData.getDurationMillis(),video_Media_MetaData.getHeight(),video_Media_MetaData.getWidth()));

                    height_list.add(video_Media_MetaData.getHeight().longValue());
                    width_list.add(video_Media_MetaData.getWidth().longValue());

                }
            }
            DurationFilter filter  = new DurationFilter();
            filteredMetaData = filter.filterOutDurations(metaDataList);
//            Long minHeigth = Collections.min(filteredMetaData, new MapComparator("height")).getHeight();
//            Long minWidth = Collections.min(filteredMetaData, new MapComparator("width")).getWidth();


        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return filteredMetaData;
    }

    private static VideoMetaData getMetaData(VideoMetaDataBuilder builder, String id, String name, String description, long size, long duration, long height, long width) {
        return builder.id(id).name(name).description(description).videoSize(size).duration(duration).height(height).width(width).build();
    }

    public static void publishMessages(Map<String,String> attributes) throws Exception {
        // [START pubsub_publish]
        TopicName topicName = TopicName.of("winter-pivot-192220", "frontend-topic");
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().putAllAttributes(attributes).build();

            // Once published, returns a server-assigned message id (unique within the topic)
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

    public StringBuilder receiveMessages() throws InterruptedException {
        TopicName topic = TopicName.of("winter-pivot-192220", "frontend-topic");
        SubscriptionName subscription = SubscriptionName.of("winter-pivot-192220", "filter-subscription");
        StringBuilder display = new StringBuilder("default");
        MessageReceiver receiver =
                new MessageReceiver() {
                    @Override
                    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                        Map<String, String> attributesMap = message.getAttributesMap();
                        //attributesReceived.putAll(attributesMap);
                        List<VideoMetaData> listFiles= filter(attributesMap);
                        Map<String,String> attributes=null;

                        for(VideoMetaData videoMetaData:listFiles){
                            attributes=new HashMap<String, String>();
                            attributes.put("access_token",attributesMap.get("access_token"));
                            attributes.put("video_id",videoMetaData.getId());
                            attributes.put("video_name",videoMetaData.getName());
                            attributes.put("video_size",String.valueOf(videoMetaData.getVideoSize()));
                            display.append(videoMetaData.getName());
                            display.append(videoMetaData.getDuration());
                            display.append(videoMetaData.getId());
                            display.append(videoMetaData.getVideoSize());
                        }



                        consumer.ack();
                    }
                };
        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscription, receiver).build();
            subscriber.addListener(
                    new Subscriber.Listener() {
                        @Override
                        public void failed(Subscriber.State from, Throwable failure) {
                            // Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
                            System.err.println(failure);
                        }
                    },
                    MoreExecutors.directExecutor());
            subscriber.startAsync().awaitRunning();

            // In this example, we will pull messages for one minute (60,000ms) then stop.
            // In a real application, this sleep-then-stop is not necessary.
            // Simply call stopAsync().awaitTerminated() when the server is shutting down, etc.
            Thread.sleep(60000);
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync().awaitTerminated();
            }
        }
        return display;

    }

    private StringBuilder readFromUrl(URLConnection conn) throws IOException {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            StringBuilder content = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
            return content;
        }
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
    }
}


