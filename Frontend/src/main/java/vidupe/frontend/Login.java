package vidupe.frontend;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import vidupe.constants.EntityProperties;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@WebServlet("/login")
public class Login extends HttpServlet{
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
            Map<String,String> attributes= new HashMap<>();
            String ifExists = createEntity(data);
            attributes.put("access_token",accessToken);
            attributes.put("client_id",data.getId());
            attributes.put("email",data.getEmail());
            attributes.put("ifExists",ifExists);
            publishMessages(attributes);

            response.getWriter().print(data.getEmail()+ " "+accessToken);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private String createEntity(GoogleAccount data) {

        Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
        Key key = datastore.newKeyFactory().setNamespace("vidupe").setKind("users").newKey(data.getEmail());
        String ifExists = "false";
        Entity task = Entity.newBuilder(key)
                .set(EntityProperties.USER_ID,data.getId())
                .set(EntityProperties.NAME, StringValue.newBuilder(data.getName()).setExcludeFromIndexes(true).build())
                .set(EntityProperties.EMAIL_ID,data.getEmail())
                .set(EntityProperties.TOTAL_VIDEOS, 0)
                .set(EntityProperties.VIDEOS_PROCESSED, 0)
                .set(EntityProperties.CREATED, Timestamp.now())
                .set(EntityProperties.DONE, false)
                .build();
        try {
            datastore.add(task);
        } catch (DatastoreException ex) {
            if ("ALREADY_EXISTS".equals(ex.getReason())) {
                // entity.getKey() already exists
                ifExists = "true";
                resetEntityProperty(datastore, key, task);
            }
        }
      return ifExists;
    }

    private void resetEntityProperty(Datastore datastore, Key key, Entity task) {
        Entity newtask = Entity.newBuilder(key)
                .set(EntityProperties.USER_ID,task.getString(EntityProperties.USER_ID))
                .set(EntityProperties.NAME, StringValue.newBuilder(task.getString(EntityProperties.NAME)).setExcludeFromIndexes(true).build())
                .set(EntityProperties.EMAIL_ID,task.getString(EntityProperties.EMAIL_ID))
                .set(EntityProperties.TOTAL_VIDEOS, 0)
                .set(EntityProperties.VIDEOS_PROCESSED, 0)
                .set(EntityProperties.CREATED, task.getTimestamp(EntityProperties.CREATED))
                .set(EntityProperties.DONE, false)
                .build();
        datastore.put(newtask);

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