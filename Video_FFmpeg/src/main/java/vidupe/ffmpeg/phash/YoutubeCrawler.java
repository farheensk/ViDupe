package vidupe.ffmpeg.phash;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class YoutubeCrawler {

    public static void main(String args[]) throws IOException {
        String pathname = "/home/farheen/IdeaProjects/video_ffmpeg/src/main/java/com/uw/jsons";
        HashMap<String,String> idsDurations =  new HashMap<>();
        for(int i=1 ;i<=5;i++){
          HashMap<String,String> idDurationSet1 =  parseJsonOfTrailers(pathname + "/commercials"+i+".json");
            idsDurations.putAll(idDurationSet1);
        }

        for(int i=1 ;i<=2;i++){
            HashMap<String,String> idDurationSet1 =  parseJsonOfTrailers(pathname + "/shortfilms"+i+".json");
            idsDurations.putAll(idDurationSet1);
        }

        HashMap<String,String> idDurationSet1 =  parseJsonOfTrailers(pathname + "/borobox.json");
        idsDurations.putAll(idDurationSet1);

        //System.out.println(idsDurations);
        for(Map.Entry<String, String> en: idsDurations.entrySet()) {

            System.out.print(en.getKey());
          System.out.println(en.getValue());
        }
   }

    private static HashMap<String, String> parseJsonOfTrailers(String pathname) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        File newState = new File(pathname);
        JsonNode root = objectMapper.readTree(newState);
        JsonNode d = root.path("contents").path("twoColumnSearchResultsRenderer").path("primaryContents").path("sectionListRenderer").path("contents");
       // System.out.println(d.toString());
        HashMap<String,String> idDurationSets = new HashMap<String, String>();
        ArrayList<String> durations = new ArrayList<>();
        if (d.isArray()) {
            for (final JsonNode objNode : d) {
                JsonNode node2 = objNode.path("itemSectionRenderer").path("contents");
                if (node2.isArray()) {
                    for (final JsonNode objNode2 : node2) {
                        JsonNode id = objNode2.path("videoRenderer").path("videoId");
                        JsonNode duration = objNode2.path("videoRenderer").path("lengthText").path("simpleText");
                        if(duration.toString() != "")
                             idDurationSets.put(id.toString(),duration.toString());
                                            }
                                        }
                                    }
                                }
        return idDurationSets;
    }

    private static HashMap<String,String> parseJson(String pathname) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        File newState = new File(pathname);
        JsonNode root = objectMapper.readTree(newState);
        JsonNode d = root.path("contents").path("twoColumnBrowseResultsRenderer").path("tabs");
        String duration = root.path("contents").path("twoColumnBrowseResultsRenderer").path("tabs").path("tabRenderer").path("content").path("sectionListRenderer").path("contents").path("itemSectionRenderer").path("contents").path("shelfRenderer").path("content").path("gridRenderer").path("items").path("gridVideoRenderer")
                .path("thumbnailOverlays").path("simpleText").toString();
      //  System.out.println(d.getNodeType() + "\n " + duration);
        HashMap<String,String> idDurationSets = new HashMap<String, String>();
        ArrayList<String> durations = new ArrayList<>();
        if (d.isArray()) {
            for (final JsonNode objNode : d) {
                JsonNode node2 = objNode.path("tabRenderer").path("content").path("sectionListRenderer").path("contents");
                if (node2.isArray()) {
                    for (final JsonNode objNode2 : node2) {
                        JsonNode node3 = objNode2.path("itemSectionRenderer").path("contents");
                        if (node3.isArray()) {
                            for (final JsonNode objNode3 : node3) {
                                JsonNode node4 = objNode3.path("shelfRenderer").path("content").path("gridRenderer").path("items");
                                if (node4.isArray()) {
                                    for (final JsonNode objNode4 : node4) {
                                        JsonNode node5 = objNode4.path("gridVideoRenderer").path("thumbnailOverlays");
                                        //JsonNode videoIdNode = objNode4.path("gridVideoRenderer").path("navigationEndpoint").path("watchEndpoint").path("videoId");
                                        JsonNode videoIdNode = objNode4.path("gridVideoRenderer").path("videoId");
//                                        System.out.print(videoIdNode + " ");
                                        String videoId = videoIdNode.toString();
                                        //System.out.print(videoId);
                                        if (node5.isArray()) {
                                            for (final JsonNode objNode5 : node5) {
                                                JsonNode node6 = objNode5.path("thumbnailOverlayTimeStatusRenderer").path("text").path("simpleText");
                                                String videoDuration = node6.toString();
                                                durations.add(node6.toString());
                                                if(videoDuration != "")
                                                    idDurationSets.put(videoId,node6.toString());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        //System.out.println(durations);
        return idDurationSets;
    }
}