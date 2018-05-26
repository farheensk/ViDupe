<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<%@page import="vidupe.frontend.Results" %>
<%@page import="vidupe.frontend.DuplicateVideosList" %>
<%@page import="vidupe.frontend.VideoHashesInformation" %>
<%@page import="vidupe.frontend.CombinedDuplicatesList"%>
<%@page import="java.util.HashMap"%>
<%@page import="com.fasterxml.jackson.databind.ObjectMapper" %>
<%@page import="java.util.concurrent.TimeUnit"%>

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
 <title>ViDupe- Duplicate Video Detection Service- Results</title>
<link rel="stylesheet" href="style2.css">
</head>
<body>
<div class='background-top'>
<h1 class='displayText'>ViDupe- Duplicate Video Detection Service</h1>
</div>
<div class="signOut">
<h3>Videos with green border indicate good quality videos</h3>
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
  <section class="">
 <div class="container">
  <table class="table table-bordered">
  <thead>
          <tr>
            <th>
                 Video
                 <div>Video</div>
            </th>
            <th>
              Duplicates
              <div style="text-align:center">Duplicates</div>
            </th>
          </tr>
        </thead>
  <tbody>
    <tr>
    <%
     for(DuplicateVideosList dup:duplicatesList.getDuplicateVideosList()){
     String referenceVideoId = dup.getReferenceVideo().getVideoID();
     String url = thumbnailsList.get(referenceVideoId);
     HashMap<String,String> bestVideoIds = dup.getBestVideoIds();
     %>
                <td>
                <div class="video" style="width:170px;,height:150px;">
                <%
                if(bestVideoIds.get(referenceVideoId)== null){
                %>
                    <img src="<%=url%>" style="max-height: 80px;max-width: 140px;"></img>
                <%
                }
                else {
                %>
                     <img src="<%=url%>" border="5" style="border-color:green;max-height: 70px;max-width: 140px;"></img>
                <%
                }
                %>
                    <br>
                    <input type='checkbox' name='video_array' value= '<%=dup.getReferenceVideo().getVideoID()%>' >
                    <%=dup.getReferenceVideo().getVideoName()%><br>
                   Resolution:<%=dup.getReferenceVideo().getResolution()%><br>
                   Duration:<%=dup.getReferenceVideo().getDurationString()%></input>
                </td>
                 <td>
                    <div class="duplicates" style="overflow-x: scroll;position: relative;width: 580px;height:200px">
                      <table class="inner-table">
                        <tbody>
                        <tr>
     <%           for(VideoHashesInformation videoHashes: dup.getDuplicateVideosList()){
                  String dupVideoID = videoHashes.getVideoID();
                  String url1 = thumbnailsList.get(dupVideoID);

     %>
                    <td>
                             <div class="video">
                             <%
                             if(bestVideoIds.get(dupVideoID)== null){
                             %>
                             <img src="<%=url1%>" style="max-height: 80px;max-width: 140px;"></img>
                            <%
                                }
                                else {
                            %>
                             <img src="<%=url1%>" border="5" style="border-color:green;max-height: 80px;max-width: 140px;"></img>
                             <%
                             }
                             %>
                             </br>
                             <input type='checkbox' name='video_array' value= '<%=videoHashes.getVideoID()%>' >
                             <%=videoHashes.getVideoName()%><br>
                             Resolution:<%=videoHashes.getResolution()%><br>
                             Duration:<%=videoHashes.getDurationString()%><br>
                             </input>
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
    </section>
    <input class='loginBtn loginBtn--google' style='padding:0 15px 0 15px;' type='submit' name='submit' value='Permanent Delete'>
                                </input>
                                </form>

    </body>
    </html>