package vidupe.frontend;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import vidupe.constants.EntityProperties;
import vidupe.message.FilterMessage;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@WebServlet("/login")
public class Login extends HttpServlet{
    Datastore datastore;
    public static void publishMessages(FilterMessage message) throws Exception {
        // [START pubsub_publish]
        TopicName topicName = TopicName.of("winter-pivot-192220", "frontend-topic");
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();
            ByteString data = ByteString.copyFromUtf8(message.toJsonString());
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

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

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        try {
            // "&scope=Google_Service_Drive::DRIVE"+
            String code = request.getParameter("code");
            String urlParameters = "code=" + code + "&client_id=" + Setup.CLIENT_ID + "&client_secret="
                    + Setup.CLIENT_SECRET + "&redirect_uri=" + Setup.REDIRECT_URL + "&grant_type=authorization_code";
            URL url = new URL("https://accounts.google.com/o/oauth2/token");
            URLConnection conn = url.openConnection();
            conn.setDoOutput(true);
            OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
            writer.write(urlParameters);
            writer.flush();
            StringBuilder content = readFromUrl(conn);
            String accessToken = GsonParser.getJsonElementString("access_token", content.toString());
            url = new URL("https://www.googleapis.com/oauth2/v1/userinfo?access_token=" + accessToken);
            conn = url.openConnection();
            StringBuilder content1 = readFromUrl(conn);
            GoogleAccount data = new Gson().fromJson(content1.toString(), GoogleAccount.class);
            String jobId = getUuid();
            String ifExists = createEntity(data, jobId);
            addAccessTokenToDataStore(jobId, data.getEmail(), accessToken);
            FilterMessage mesesage = FilterMessage.builder()
                    .jobId(jobId)
                    .accessToken(accessToken)
                    .clientId(data.getId())
                    .email(data.getEmail())
                    .ifExists(ifExists)
                    .build();
            publishMessages(mesesage);
            String storageURL = "https://storage.cloud.google.com/vidupe/";
            String encodedEmail = URLEncoder.encode(data.getEmail(),"UTF-8");
            String resultsURL = storageURL+encodedEmail+"/"+jobId;
            System.out.println(resultsURL);
            response.sendRedirect(request.getContextPath() + "/Results?jobid="+jobId+"&email="+data.getEmail());
            } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void addAccessTokenToDataStore(String jobId, String email, String accessToken) {
        this.datastore = DatastoreOptions.newBuilder().setNamespace("vidupe").build().getService();
        Key key = createTokenKey(jobId,email);
        Entity entity = Entity.newBuilder(key)
                .set("accessToken", accessToken).build();
        datastore.put(entity);

    }

    private Key createTokenKey(String jobId, String email) {
        Key key = datastore.newKeyFactory()
                .setKind("tokens")
                .addAncestors(PathElement.of("user", email))
                .newKey(jobId);
        return key;
    }

    public String getUuid() {
        UUID idOne = UUID.randomUUID();
        String UUIDinString = idOne.toString();
        String jobID = UUIDinString.substring(0,8)+UUIDinString.substring(9,13)
                +UUIDinString.substring(14,18)+UUIDinString.substring(19,23)+UUIDinString.substring(24);
        return jobID;
    }

    private String createEntity(GoogleAccount data, String jobId) {

        this.datastore = DatastoreOptions.newBuilder().setNamespace("vidupe").build().getService();
        Key key = createKey(jobId, data.getEmail());
               // key = datastore.newKeyFactory().setKind("users").newKey(data.getEmail());
        String ifExists = "false";
        Entity task = Entity.newBuilder(key)
                .set(EntityProperties.USER_ID,data.getId())
                .set(EntityProperties.NAME, data.getName())
                .set(EntityProperties.EMAIL_ID,data.getEmail())
                .set(EntityProperties.TOTAL_VIDEOS, 0)
                .set(EntityProperties.CREATED, Timestamp.now())
                .set(EntityProperties.DONE, false)
                .build();
        try {
            datastore.add(task);
        } catch (DatastoreException ex) {
            if ("ALREADY_EXISTS".equals(ex.getReason())) {
                // entity.getKey() already exists
                ifExists = "true";
                resetEntityProperty(datastore, key, task, jobId);
            }
        }
      return ifExists;
    }

    public Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("users")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }
    private void resetEntityProperty(Datastore datastore, Key key, Entity task, String jobId) {
        Entity newtask = Entity.newBuilder(key)
                .set(EntityProperties.USER_ID,task.getString(EntityProperties.USER_ID))
                .set(EntityProperties.NAME, task.getString(EntityProperties.NAME))
                .set(EntityProperties.EMAIL_ID,task.getString(EntityProperties.EMAIL_ID))
                .set(EntityProperties.TOTAL_VIDEOS, 0)
                .set(EntityProperties.CREATED, task.getTimestamp(EntityProperties.CREATED))
                .set(EntityProperties.DONE, false)
                .build();
        datastore.put(newtask);

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