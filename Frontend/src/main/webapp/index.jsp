<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta name="google-site-verification" content="iYhWKv4VCjfVlWE5UxNvHJaKi7cqovfVog3NsW4hwQc" />
        <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
        <title>ViDupe- Duplicate Video Detection Service</title>
        <link rel="stylesheet" href="style.css">
        <script type="text/javascript">
            function loadUrl()
            {
                window.location.href = "https://accounts.google.com/o/oauth2/auth?scope=email%20https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive&access_type=offline&redirect_uri=http://vidupe.tk/login&response_type=code&client_id=790841820104-151n2hqqm6bcajs857snsu1gn9l6ecc7.apps.googleusercontent.com&approval_prompt=force"
            }
            function click() {
            if (event.button==2) {
            alert('No clicking!')
            }
            }
            document.onMouseDown=click
        </script>
        <script src="https://apis.google.com/js/platform.js" async defer></script>
        <meta name="google-signin-client_id" content="790841820104-151n2hqqm6bcajs857snsu1gn9l6ecc7.apps.googleusercontent.com">
    </head>
    <body onMouseDown=click>
    <div class="background-top">
            <h1 class="displayText">ViDupe- Duplicate Video Detection Service</h1>
    </div>
    <div>
            <h3>ViDupe scans the user&#39;s google drive to detect duplicate video files. </br>
             Video files that are recently uploaded to the google drive are not scanned, since video related
            information which will not be available immediately after upload. </h3>
            <hr>
            <a href="#" id="load" onclick="loadUrl()">
               <!-- <button class="loginBtn loginBtn--google">Sign in with Google</button>
                </br>-->
                <input type="image" src="red_login3.png" />
            </a>
           <!-- <button class="loginBtn loginBtn--google">Click here to view privacy policy</button>
            Click <a href="/ViDupe_Privacy_Policy.html">
            here
            <input type="image" src="image2.png" />
            </a>
            to view the privacy policy of ViDupe -->
     </div>
     <div class="foot">
            <h3>click the button below to view the privacy policy of ViDupe</h3>
            <a href="/ViDupe_Privacy_Policy.html">
            <input type="image" src="3.png" class="loginBtn loginBtn--google"></input>
            </a>
             <hr>
             &copy; 2018 University of Washington
         </div>
    </body>
</html>