package vidupe.frontend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.List;


@WebServlet("/Results")
public class Results extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(Delete.class);

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        String jobId = request.getParameter("jobid");
        String email = request.getParameter("email");
        logger.info("Waiting for results: jobId:"+jobId+" ,email:  "+email);
        String storageURL = "https://storage.cloud.google.com/vidupe/";
        String encodedEmail = URLEncoder.encode(email, "UTF-8");
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get("vidupe", email + "/" + jobId);
        boolean isServletNeedRefresh = true;
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println(
                "<html>" +
                        "<head>" +
                        "<title>Results</title>" +
                        "<link rel='stylesheet' href='style.css'>" +
                        "</head>" +
                        "<body>" +
                        "<div class='background-top'>" +
                        "<h1 class='displayText'>ViDupe- Duplicate Video Detection Service</h1>" +
                        "</div>" +
                        "<div>"

        );
        if (blob != null && blob.exists()) {
            logger.info("Displaying results: jobId:"+jobId+" ,email:  "+email);
            isServletNeedRefresh = false;
            out.println("<a style='width:400px;margin:auto;padding-top:30px;' href='https://mail.google.com/mail/u/0/?logout&hl=en'>");
            out.println("<button class='loginBtn loginBtn--google' style='float:right;'>Sign Out</button></a>" +
                    "</div><div>");
            out.println("<form action='" + request.getContextPath() + "/delete?email=" + email + "&jobid=" + jobId + "' method='get'>");
            out.println("<input type='hidden' name='email' value='" + email + "'/>");
            out.println("<input type='hidden' name='jobid' value='" + jobId + "'/>");

            out.println("<table>");
            out.println("<tbody>");
            ByteString content = ByteString.copyFrom(blob.getContent());
            String messageString = content.toStringUtf8();
            ObjectMapper mapper = new ObjectMapper();
            DuplicateVideosList duplicateVideosList = mapper.readValue(messageString, DuplicateVideosList.class);
            if (duplicateVideosList.getDuplicateVideosList() != null) {
                if (duplicateVideosList.getDuplicateVideosList().size() > 0) {
                    for (List<VideoHashesInformation> duplicateGroup : duplicateVideosList.getDuplicateVideosList()) {
                        out.println("<tr style='max-height: 150px; max-width: 150px'>" +
                                "<td>Duplicates Set</td>");
                        for (VideoHashesInformation video : duplicateGroup) {
                            String url = video.getThumbnailLink();
                            out.println("<td>");
                            out.println("<img src='" + url + "'style='max-height: 150px; max-width: 150px;'></img></br>" +
                                    "<input type='checkbox' name='video_array' value='" + video.getVideoID() + "'>" + video.getVideoName() + "</input></td>");
                        }
                        out.println("</tr>");
                    }
                    out.println("</tbody></table>" +
                            "<input class='loginBtn loginBtn--google' style='padding:0 15px 0 15px;' type='submit' name='submit' value='Permanent Delete'>" +
                            "</input>" +
                            "</form>");
                }
            }
            else {
                out.println("<p> you don't have any duplicate video files in your drive </p>");
            }
            out.println("</div>");
        } else {
            isServletNeedRefresh = true;
            String loadingGif = "loading.gif";
            out.println("<h3>Loading your results...please wait...</h3>");
            out.println("<img src='" + loadingGif + "' style='max-height:150px;max-width:150px;align:middle;'></img>");

        }
        if (isServletNeedRefresh)
            response.setIntHeader("Refresh", 5);
        out.println("</div></body>");
        out.println("</html>");
        out.close();
    }
}
