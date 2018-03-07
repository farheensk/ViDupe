package vidupe.filter;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.collect.Iterators;
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
import java.net.URLConnection;
import java.util.*;
import java.util.regex.Pattern;

@WebServlet("/filter")
public class FD extends HttpServlet {
    public static void publishMessages(Map<String,String> attributes) throws Exception {
        // [START pubsub_publish]
        TopicName topicName = TopicName.of("winter-pivot-192220", "filter-topic");
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().putAllAttributes(attributes).build();
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

    private static VideoMetaData getMetaData(VideoMetaData.VideoMetaDataBuilder builder, String id, String name, String description, DateTime dateModified,long size, long duration, long height, long width) {
        return builder.id(id).name(name).description(description).dateModified(dateModified).videoSize(size).duration(duration).height(height).width(width).build();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {

            StringBuilder display = receiveMessages();
            response.getWriter().print(display);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<VideoMetaData>  filter(Map<String,String> attributesReceived){
        List<VideoMetaData>  filteredMetaData = null;
        try{
            String accessToken = attributesReceived.get("access_token");
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
                if(Pattern.matches("video/.*", type)){

                    VideoMetaData.VideoMetaDataBuilder builder = VideoMetaData.builder();
                    File.VideoMediaMetadata video_Media_MetaData = file.getVideoMediaMetadata();
                    // DateTime modifiedTime = file.getModifiedTime();
                    metaDataList.add(getMetaData(builder, file.getId(), file.getName(),file.getDescription(), file.getModifiedTime(), file.getSize(),
                            video_Media_MetaData.getDurationMillis(),video_Media_MetaData.getHeight(),video_Media_MetaData.getWidth()));

                    height_list.add(video_Media_MetaData.getHeight().longValue());
                    width_list.add(video_Media_MetaData.getWidth().longValue());

                }
            }
            DurationFilter filter  = new DurationFilter();
            filteredMetaData = filter.filterOutDurations(metaDataList);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return filteredMetaData;
    }

    public StringBuilder receiveMessages() {
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
                        Map<String,String> attributes;
                        Long minHeigth = Collections.min(listFiles, new MapComparator("height")).getHeight();
                        Long minWidth = Collections.min(listFiles, new MapComparator("width")).getWidth();
                        Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

//                      // Create a Key factory to construct keys associated with this project.
                        KeyFactory keyFactory = datastore.newKeyFactory().setNamespace("user-video-info").setKind(attributesMap.get("clientId"));

                        for(VideoMetaData videoMetaData:listFiles){

                            boolean canProcess = createEntity(datastore,keyFactory,videoMetaData);
                            if(canProcess){
                                attributes= new HashMap<>();
                                attributes.put("access_token",attributesMap.get("access_token"));
                                attributes.put("video_id",videoMetaData.getId());
                                attributes.put("video_name",videoMetaData.getName());
                                attributes.put("video_size",String.valueOf(videoMetaData.getVideoSize()));
                                attributes.put("video_duration",String.valueOf(videoMetaData.getDuration()));

                                attributes.put("min_height",minHeigth.toString());
                                attributes.put("min_width",minWidth.toString());

                                try {
                                    publishMessages(attributes);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        deleteEntityIfNotExists(attributesMap,keyFactory,datastore);
                        consumer.ack();
                    }
                };
        Subscriber subscriber = Subscriber.newBuilder(subscription, receiver).build();
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

        return display;

    }

    private void deleteEntityIfNotExists(Map<String, String> attributesMap, KeyFactory keyFactory,Datastore datastore) {
        Query<Key> query = Query.newKeyQueryBuilder().setNamespace("user-video-info").setKind(attributesMap.get("clientId")).
                setFilter(StructuredQuery.PropertyFilter.eq("found", false))
                .build();
        QueryResults<Key> result = datastore.run(query);
        datastore.delete(Iterators.toArray(result, Key.class));

    }


    private boolean createEntity(Datastore datastore, KeyFactory keyFactory,VideoMetaData videoMetaData) {

        Key key = keyFactory.newKey(videoMetaData.getId());
        Entity task = Entity.newBuilder(key)
                .set("video-name",videoMetaData.getName())
                .set("duration", videoMetaData.getDuration())
                .set("created", Timestamp.now())
                .set("hashes","")
                .set("found",true)
                .set("done", false)
                .build();
        if(datastore.get(key) == null) {
            Entity addedEntity = datastore.add(task);
            return addedEntity != null;
        } else {
            Entity video = datastore.get(key);
            Timestamp created = video.getTimestamp("created");
            DateTime dateModified = videoMetaData.getDateModified();
            boolean needToUpdateEntity = compareTimes(created, dateModified);
            if(needToUpdateEntity) {
                datastore.put(task);
                return true;
            }
            else {
                task = Entity.newBuilder(datastore.get(key)).set("found", "true").build();
                datastore.update(task);
                return false;
            }
        }
    }

    boolean compareTimes(Timestamp created, DateTime dateModified) {
        long modifiedMillis = dateModified.getValue();
        long createdMillis = created.getSeconds()*1000;
        return createdMillis < modifiedMillis;
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


