package vidupe.frontend;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.UUID;

public class LoginTest{

    @Test
    public void getUuid() {
        UUID idOne = UUID.randomUUID();
        String UUIDinString = idOne.toString();
        String jobID = UUIDinString.substring(0,8)+UUIDinString.substring(9,13)
                +UUIDinString.substring(14,18)+UUIDinString.substring(19,23)+UUIDinString.substring(24);
        System.out.println(UUIDinString);
        System.out.println(jobID);
    }

    @Test
    public void urlEncodingTest() throws UnsupportedEncodingException {
        String email = "farheen_sk@gmail.com";
        String jobId = "12425";
        String resultsUrl = "https://storage.cloud.google.com/vidupe/";
        String encodedEmail = URLEncoder.encode(email,"UTF-8");
        System.out.println(resultsUrl+encodedEmail+"/"+jobId);
    }
}