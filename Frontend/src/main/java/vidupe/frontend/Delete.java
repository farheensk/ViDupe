package vidupe.frontend;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.cloud.datastore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

@WebServlet("/delete")
public class Delete extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(Delete.class);

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String jobId = request.getParameter("jobid");
        String email = request.getParameter("email");
        logger.info("Preparing to delete videos of : jobId:"+jobId+" ,email:  "+email);

        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println(
                "<html>"+
                        "<head>"+
                        "<title>Results</title>" +
                        "<link rel='stylesheet' href='style.css'>"+
                        "</head>"+
                        "<body>" +
                        "<div class='background-top'>" +
                        "<h1 class='displayText'>ViDupe- Duplicate Video Detection Service</h1>" +
                        "</div>"+
                        "<div class='display-section'>"
        );
        if(request.getParameterValues("video_array")!=null){
            String [] result = request.getParameterValues("video_array");
            if(result.length>0){
                logger.info("Deleting videos of : jobId:"+jobId+" ,email:  "+email);

                final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE);
                Datastore datastore = DatastoreOptions.newBuilder().setNamespace("vidupe").build().getService();
                Key key = datastore.newKeyFactory()
                        .setKind("tokens")
                        .addAncestors(PathElement.of("user", email))
                        .newKey(jobId);
                Entity entity= datastore.get(key);
                String accessToken=entity.getString("accessToken");
                GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken).createScoped(SCOPES);

                Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
                        .setApplicationName("Duplicate video Detection").build();

                for(String fileId:result){
                    try {
                        drive.files().delete(fileId).execute();
                    } catch (IOException e) {
                        out.println("<p> Error in deleting the file </p>");
                    }
                }
                out.println("<p>Successfully deleted video files</p>");
            }
        }

        else{
            out.println("<p>No files to delete </p>");
        }
        out.println("</div></body>");
        out.println("</html>");
        out.close();

    }
}
