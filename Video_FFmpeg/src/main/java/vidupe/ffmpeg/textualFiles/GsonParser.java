package vidupe.ffmpeg.textualFiles;

//import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class GsonParser
{
 
  public static String getJsonElementString(String name, String gs)
  {
    try
    {
      JsonObject json = (JsonObject)new JsonParser().parse(gs);
      return json.get(name).getAsString();
    }
    catch (Exception localException) {}
    return null;
  }
}
