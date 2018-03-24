<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<html>
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
    <title>Insert title here</title>
    <link rel="stylesheet" href="style.css">
    </head>
    <body>
        <div style="width:400px;margin:auto;padding-top:30px;">
           <p> Results Path <%=request.getAttribute("path")%> </p>

           <a href=<%=request.getAttribute("path")%>>View Results</a>
           <a style="width:400px;margin:auto;padding-top:30px;" href="https://mail.google.com/mail/u/0/?logout&hl=en">
             <button class="loginBtn loginBtn--google">Sign Out</button></a>
        </div>
    </body>
</html>