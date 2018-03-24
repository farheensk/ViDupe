<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<%@page import="vidupe.frontend.VideoHashesInformation" %>
<%@page import="vidupe.frontend.DuplicateVideosList" %>

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Insert title here</title>
<link rel="stylesheet" href="style.css">
</head>
<body>
 <jsp:useBean id="Bean" class="com.uw.vidupe.GoogleAccount"/>
  <jsp:setProperty property="*" name="Bean"/>
<%GoogleAccount gp = (GoogleAccount)request.getAttribute(Constants.AUTH); %>
<div style="width:400px;margin:auto;padding-top:30px;">
  <table class="table table-bordered">
    <tr>
      <td>User ID</td>
      <td><%=gp.getId()%></td>
    </tr>
    <tr>
      <td>Name</td>
      <td><%=gp.getName()%></td>
    </tr>
    <tr>
      <td>Email</td>
      <td><%=gp.getEmail()%></td>
    </tr>
   <!-- <tr>
      <td>Filtered_ids</td>
      <td><%=request.getAttribute("filtered_ids")%></td>
    </tr> -->
     <tr>
      <td>Filtered_names</td>
      <td><%=request.getAttribute("filtered_names")%></td>
    </tr>
     <tr>
      <td>Duplicates</td>
      <td><%=request.getAttribute("Duplicate_Items")%></td>
    </tr>
      <!-- <tr>
      <td>Drive</td>
      <td><%=request.getAttribute("drive")%></td>
    </tr> -->
  </table>

 <%-- <table class="table2">

     <%

   		int len=Integer.parseInt(request.getAttribute("length").toString());
   		FilteredVideo[]  filtered=new FilteredVideo[len];
        filtered=Bean.getFilteredVideoFiles();
        out.println(len+" "+filtered);
   		%>

  	  <tr>
      <td>ID</td>

      <td><%=filtered[0].getVideoID()%></td>
   	</tr>
   	<tr>
      <td>Name</td>
      <td><%=filtered[0].getVideoName()%></td>
   	</tr>
   	<tr>
   <tr>
      <td>Description</td>
      <td><%=filtered[0].getDescription()%></td>
   	</tr>
  </table>  --%>
  <a	style="width:400px;margin:auto;padding-top:30px;" href="https://mail.google.com/mail/u/0/?logout&hl=en">
	 <button class="loginBtn loginBtn--google">Sign Out</button></a>


</div>

<!-- <div class="loginBtn loginBtn--google">
<input type="button" onclick="location.href='https://mail.google.com/mail/u/0/?logout&hl=en';" value="Sign Out" />
</div> -->
</body>
</html>