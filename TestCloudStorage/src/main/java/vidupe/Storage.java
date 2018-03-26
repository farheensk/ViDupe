package vidupe;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.StorageOptions;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;

import static org.apache.http.protocol.HTTP.UTF_8;

@WebServlet("/Storage")
public class Storage extends HttpServlet {
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String text = "Some data to cloud......";
        File file = new File("farheen/hello1.txt");
        com.google.cloud.storage.Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get("vidupe");
        Blob blob = bucket.create("farheen/my_blob_name", text.getBytes(UTF_8), "application/json");
        Blob blob1 = storage.get("vidupe", "farheen/my_blob_name");
        String fileContent = new String(blob1.getContent());
        response.getWriter().print(fileContent);
//        List<Acl> acls = new ArrayList<>();
//        acls.add(Acl.of(Acl.User.ofAllUsers(), Acl.Role.OWNER));
//        try (PrintWriter out = new PrintWriter(file)) {
//            out.println(text);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        try {
//            blob = storage.create(
//                    BlobInfo.newBuilder("vidupe", file.getName()).setAcl(acls).build(), new FileInputStream(file));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
}
