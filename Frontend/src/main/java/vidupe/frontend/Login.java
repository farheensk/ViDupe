package vidupe.frontend;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.constants.Constants;
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
    private static final Logger logger = LoggerFactory.getLogger(Delete.class);

    public static void publishMessages(FilterMessage message) throws Exception {
        // [START pubsub_publish]
        TopicName topicName = TopicName.of(Constants.PROJECT, Constants.FRONTEND_TOPIC);
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
                logger.info("Published message. messageId=" + messageId + ", jobId=" + message.getJobId());
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
            VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService());
            String ifExists =vidupeStoreManager.createEntity(data, jobId);
            vidupeStoreManager.addAccessTokenToDataStore(jobId, data.getEmail(), accessToken);
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
            logger.info("Results_available_at "+resultsURL);
            response.sendRedirect(request.getContextPath() + "/Results?jobid="+jobId+"&email="+data.getEmail());
            } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getUuid() {
        UUID idOne = UUID.randomUUID();
        String UUIDinString = idOne.toString();
        String jobID = UUIDinString.substring(0,8)+UUIDinString.substring(9,13)
                +UUIDinString.substring(14,18)+UUIDinString.substring(19,23)+UUIDinString.substring(24);
        return jobID;
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