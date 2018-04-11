<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<%@page import="vidupe.frontend.Results" %>
<%@page import="vidupe.frontend.DuplicateVideosList" %>
<%@page import="vidupe.frontend.VideoHashesInformation" %>
<%@page import="vidupe.frontend.CombinedDuplicatesList"%>
<%@page import="java.util.HashMap"%>
<%@page import="com.fasterxml.jackson.databind.ObjectMapper" %>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Insert title here</title>
<link rel="stylesheet" href="style.css">

</head>
<body>
<div class='background-top'>
<h1 class='displayText'>ViDupe- Duplicate Video Detection Service</h1>
</div>
<div class="signOut">
<a style='width:400px;margin:auto;padding-top:30px;' href='https://mail.google.com/mail/u/0/?logout&hl=en'>
<button class='loginBtn loginBtn--google' style='float:right;'>Sign Out</button></a>
</div>

<%
 ObjectMapper mapper = new ObjectMapper();
 String data = request.getAttribute("results").toString();
 CombinedDuplicatesList duplicatesList = mapper.readValue(data, CombinedDuplicatesList.class);
  HashMap<String,String> thumbnailsList= duplicatesList.getThumbnails();
  String email = request.getAttribute("email").toString();
  String jobId = request.getAttribute("jobId").toString();
 %>
 <form action="/delete?email=<%=email%>&jobid=<%=jobId%>" method="get">
  <input type='hidden' name='email' value="<%=email%>">
  <input type='hidden' name='jobid' value="<%=jobId%>">
 <div>
  <table class="table table-bordered">
  <tbody>
  <tr>
      <td> <h2>Reference <br> Video </h2> </td>
      <td> <h2> Duplicates </h2> </td>
  </tr>
    <tr>
    <%
     for(DuplicateVideosList dup:duplicatesList.getDuplicateVideosList()){
     String url = thumbnailsList.get(dup.getReferenceVideo().getVideoID());
     %>
                <td>
                <div class="video">
                    <img src="<%=url%>"></img><br>
                    <input type='checkbox' name='video_array' value= '<%=dup.getReferenceVideo().getVideoID()%>' >
                   <%=dup.getReferenceVideo().getVideoName()%></input>
                </td>
                 <td>
                    <div class="duplicates">
                      <table class="inner-table">
                        <tbody>
                        <tr>
     <%           for(VideoHashesInformation videoHashes: dup.getDuplicateVideosList()){
                  String url1 = thumbnailsList.get(videoHashes.getVideoID());
     %>
                    <td>
                             <div class="video">
                             <img src="<%=url1%>"></img>
                             </br>
                             <input type='checkbox' name='video_array' value= '<%=videoHashes.getVideoID()%>' >
                             <%=videoHashes.getVideoName()%></input>
                             </div>
                    </td>

    <% } %>

            </tr>
            </tbody>
                </table>
              </div>
                </td>
             </tr>
          <%
          }
          %>

    </tbody>
    </table>
    </div>
    <input class='loginBtn loginBtn--google' style='padding:0 15px 0 15px;' type='submit' name='submit' value='Permanent Delete'>
                                </input>
                                </form>

    </body>
    </html>