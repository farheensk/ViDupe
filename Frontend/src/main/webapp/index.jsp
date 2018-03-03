<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta name="google-site-verification" content="iYhWKv4VCjfVlWE5UxNvHJaKi7cqovfVog3NsW4hwQc" />
        <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
        <title>Insert title here</title>
        <style>
            body  {
                background-image: url("2.png");
                background-color: #cccccc;
            }
        </style>
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
        <a href="#" id="load" onclick="loadUrl()">
            <button class="loginBtn loginBtn--google">Sign in with Google</button>
        </a>
         <!-- scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive.metadata.readonly&
         access_type=offline& -->
    </body>
</html>