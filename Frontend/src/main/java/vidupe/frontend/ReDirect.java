package vidupe.frontend;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;

@WebServlet("/redirect")
public class ReDirect extends HttpServlet {
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        URL url = new URL("https://accounts.google.com/o/oauth2/auth?scope=email%20https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive&access_type=offline&redirect_uri=http://vidupe.tk/login&response_type=code&client_id=790841820104-151n2hqqm6bcajs857snsu1gn9l6ecc7.apps.googleusercontent.com&approval_prompt=force");
        url.openConnection();


    }
}
