package vidupe.ffmpeg.textualFiles;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bytedeco.javacv.FFmpegFrameGrabber;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.File.VideoMediaMetadata;
import com.google.api.services.drive.model.FileList;
import com.google.gson.Gson;

import it.sauronsoftware.jave.Encoder;
import it.sauronsoftware.jave.MultimediaInfo;
import it.sauronsoftware.jave.VideoInfo;

@WebServlet("/oauth2callback")
public class OAuth2Callback extends HttpServlet {
	private static final long serialVersionUID = 1L;
	static String className = "com.abc.Oauth2callback";

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType("text/html;charset=UTF-8");
	      // Allocate a output writer to write the response message into the network socket
	      PrintWriter out = response.getWriter();
	 
		try {
			 
			// "&scope=Google_Service_Drive::DRIVE"+
			String code = request.getParameter("code");
			String urlParameters = "code=" + code + "&client_id=" + Setup.CLIENT_ID + "&client_secret="
					+ Setup.CLIENT_SECRET + "&redirect_uri=" + Setup.REDIRECT_URL + "&grant_type=authorization_code";
			URL url = new URL("https://accounts.google.com/o/oauth2/token");
			URLConnection conn = url.openConnection();
			conn.setDoOutput(true);
			OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
			writer.write(urlParameters);
			writer.flush();
			String line1 = "";
			BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = reader.readLine()) != null) {
				line1 = line1 + line;
			}
			String accessToken = GsonParser.getJsonElementString("access_token", line1);

			url = new URL("https://www.googleapis.com/oauth2/v1/userinfo?access_token=" + accessToken);
			conn = url.openConnection();
			GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken);
			Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), credential)
					.setApplicationName("Duplicate video Detection").build();

			// https://developers.google.com/apis-explorer/#p/drive/v3/drive.files.list?fields=files(id%252Cmd5Checksum%252CmimeType%252Cname%252Csize%252CvideoMediaMetadata)&_h=8&
			FileList result = drive.files().list().setFields(
					"files(capabilities/canDownload,id,md5Checksum,mimeType,name,size,videoMediaMetadata,webContentLink)")
					.execute();
			List<File> files = result.getFiles();
			FilteredVideos[] filtered_Objects = new FilteredVideos[files.size()];

			List<HashMap<String, String>> listOfFiles = new ArrayList<HashMap<String, String>>();

			int count = 0;

			String url1 = null;
			StringBuilder ids = new StringBuilder();
			StringBuilder names = new StringBuilder();
			StringBuilder descriptions = new StringBuilder();
			StringBuilder duplicates_metadata = new StringBuilder();

			for (File file : files) {
				String type = file.getMimeType();
				if (type.compareTo("video/mp4") == 0 || type.compareTo("video/avi") == 0) {
					// url1 = file.getWebContentLink();
					VideoMediaMetadata video_Media_MetaData = file.getVideoMediaMetadata();
					filtered_Objects[count] = new FilteredVideos();
					filtered_Objects[count].setVideoID(file.getId());
					filtered_Objects[count].setVideoName(file.getName());
					filtered_Objects[count].setDescription(file.getDescription());

					HashMap<String, String> map = new HashMap<String, String>();
					map.put("name",file.getName());
					map.put("id",file.getId());
					map.put("description",file.getDescription());
					map.put("size", String.valueOf(file.getSize()));
					map.put("duration", String.valueOf(video_Media_MetaData.getDurationMillis()));
					map.put("height", String.valueOf(video_Media_MetaData.getHeight().longValue()));
					map.put("width", String.valueOf(video_Media_MetaData.getWidth().longValue()));

					listOfFiles.add(map);

					ids.append(file.getId() + " ");
					names.append(file.getName() + " ");
					descriptions.append(file.getDescription() + " ");
					count++;
				}
			}
			
//			System.out.println(listOfFiles.get(0));
//			System.out.println(listOfFiles.get(1));
//			System.out.println(listOfFiles.get(2));

			HashMap<Integer, ArrayList<Integer>> dup = using_metadata(listOfFiles,count);
			Set<Integer> allKeys = dup.keySet();
			ArrayList<Integer> allVideos = new ArrayList<Integer>();
			for(int i:allKeys) {
				allVideos.add(i);
				for(int j:dup.get(i)) {
					allVideos.add(j);
				}
			}
			// String[] duplicateIds= new String[count];compareMetaData()
			GoogleDetails object1 = new GoogleDetails();
			object1.setFiles(files);
			object1.setLenght(count);

			object1.setFilteredVideoFiles(filtered_Objects);
			java.io.File dir = new java.io.File("gmail");
			dir.mkdir();
			System.out.println(filtered_Objects.length);
			for (int i:allVideos) {
					url = new URL(
							"https://www.googleapis.com/drive/v3/files/" + listOfFiles.get(i).get("id") + "?alt=media");

					HttpRequest httpRequestGet = drive.getRequestFactory().buildGetRequest(new GenericUrl(url.toString()));

					httpRequestGet.getHeaders().setRange("bytes=" + 0 + "-");
					System.out.println(httpRequestGet.getHeaders());
					// httpRequestGet.getHeaders().set("Range", "bytes="+0+"-"+200000);
					// httpRequestGet.getHeaders().set("Range", "bytes="+1000+"-"+200000);
					HttpResponse resp = httpRequestGet.execute();

					int status = resp.getStatusCode();
					OutputStream outputStream = new FileOutputStream(
							new java.io.File("gmail/" + filtered_Objects[i].getVideoName()));
					resp.download(outputStream);
					
			}
			line1 = "";
			 FFmpegFrameGrabber g=new FFmpegFrameGrabber("D://gmail//2.mp4");
			// String ii=filtered[0].getVideoID();
			reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			while ((line = reader.readLine()) != null) {
				line1 = line1 + line;
			}
			for(int i:allKeys)
			{	
				duplicates_metadata.append(listOfFiles.get(i).get("name")+", ");
				for(int j:dup.get(i)) {
					duplicates_metadata.append( listOfFiles.get(j).get("name")+ ", ");
				}	
				duplicates_metadata.append("are duplicates \n");
			}
			GoogleDetails data = (GoogleDetails) new Gson().fromJson(line1, GoogleDetails.class);
			writer.close();
			reader.close();
			request.setAttribute("length", count);
			request.setAttribute("filtered_ids", ids);
			request.setAttribute("filtered_names", names);
			request.setAttribute("filtered_descriptions", descriptions);
			request.setAttribute("auth", data);
			request.setAttribute("drive", drive);
			request.setAttribute("Duplicate_Items", duplicates_metadata);
			request.getRequestDispatcher("/google.jsp").forward(request, response);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	protected HashMap<Integer, ArrayList<Integer>> using_metadata(List<HashMap<String, String>> listOfFiles,int count) {
		HashMap<Integer, ArrayList<Integer>> duplicates = new HashMap<Integer, ArrayList<Integer>>();
		int[] flag = new int[count];
		for (int i = 0; i < flag.length; i++) {
			flag[i] = 0;
		}
		for (int i = 0; i < count; i++) {
			ArrayList<Integer> list = new ArrayList<Integer>();
			if (flag[i] == 0) {
				for (int j = i + 1; j < count; j++) {
 				if (i != j) {
//						System.out.println(listOfFiles.get(i).get("size").equals(listOfFiles.get(j).get("size")));
						if (listOfFiles.get(i).get("size").equals(listOfFiles.get(j).get("size")) && listOfFiles.get(i).get("duration").equals(listOfFiles.get(j).get("duration"))) {
							flag[j] = 1;
							list.add(j);
							System.out.println("added");
						}
					}
					if (!list.isEmpty())
						duplicates.put(i, list);
				}
			}

			System.out.println(duplicates.get(i));
		}
		duplicates.values().removeIf(Objects::isNull);
		return duplicates;
	}
	
	protected HashMap<String, ArrayList<String>> duplicate_Metadata() {
		System.out.println("Accessed");
		java.io.File folder = new java.io.File("D:\\gmail");

		ArrayList<Videos> listOfVideos = new ArrayList<Videos>();

		java.io.File[] listOfFiles = folder.listFiles();
		for (java.io.File file : listOfFiles) {
			if (file.isFile()) {
				if (file.getAbsolutePath().endsWith(".mp4") || file.getAbsolutePath().endsWith(".mov")
						|| file.getAbsolutePath().endsWith(".avi") || file.getAbsolutePath().endsWith(".x-flv")) {
					try {
						Videos v = new Videos();
						v.setName(file.getName());

						Encoder encoder = new Encoder();
						MultimediaInfo info;
						info = encoder.getInfo(file);
						VideoInfo vInfo = info.getVideo();
						v.setFrameRate(Math.round(vInfo.getFrameRate()));
						v.setLength((int) (info.getDuration() * 0.001));
						listOfVideos.add(v);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}
		}
//		for (Videos v : listOfVideos) {
//			System.out.println(v.getName() + ":" + v.getLength() + " " + v.getFrameRate());
//		}
		//System.out.println(listOfVideos);
		HashMap<Integer, ArrayList<Integer>> duplicates = new HashMap<Integer, ArrayList<Integer>>();
		int[] flag = new int[listOfVideos.size()];
		for (int i = 0; i < flag.length; i++) {
			flag[i] = 0;
		}
		for (int i = 0; i < listOfVideos.size(); i++) {
			ArrayList<Integer> list = new ArrayList<Integer>();
			if (flag[i] == 0) {
				for (int j = i + 1; j < listOfVideos.size(); j++) {
                	Videos v1 = listOfVideos.get(i);
					Videos v2 = listOfVideos.get(j);
					System.out.println(v1.getLength()+" "+v2.getLength());
					if (i != j) {
						if (v1.getLength() == v2.getLength() && v1.getFrameRate() == v2.getFrameRate()) {
							flag[j] = 1;
							list.add(j);
						}
					}
					if (!list.isEmpty())
						duplicates.put(i, list);
				}
			}

			//System.out.println(duplicates.get(i));
		}
		duplicates.values().removeIf(Objects::isNull);
		HashMap<String, ArrayList<String>> duplicate_videos = new HashMap<String, ArrayList<String>>();
		
		Set<Integer> arr = duplicates.keySet();
		for (int i : arr) {
			ArrayList<String> set_duplicates = new ArrayList<String>();
			for(int j:duplicates.get(i)) {
				set_duplicates.add(listOfVideos.get(j).getName());
			}
			duplicate_videos.put(listOfVideos.get(i).getName(), set_duplicates);
			System.out.println(i + " " + duplicates.get(i));
		}
		return duplicate_videos;
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
	}
}
