package vidupe.frontend;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.services.drive.Drive;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.*;

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
import java.util.*;

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
            BufferedReader reader;
            String line;
            String accessToken = GsonParser.getJsonElementString("access_token", content.toString());

//            url = new URL("https://www.googleapis.com/oauth2/v1/userinfo?access_token=" + accessToken);
//            conn = url.openConnection();
//            GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken);
//            Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
//                    .setApplicationName("Duplicate video Detection").build();

            Map<String,String> attributes=new HashMap<String, String>();
            attributes.put("access_token",accessToken);
            //response.getWriter().print(drive);

            publishMessages(attributes);
        } catch (Exception e) {
            e.printStackTrace();
        }


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