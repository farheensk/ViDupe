package vidupe.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@JsonIgnoreProperties({ "hashes", "audioHashes" })
public class VideoHashesInformation {
    String videoID;
    String videoName;
    long duration;
    String durationString;
    long videoSizeLong;
    long videoWidth;
    long videoHeight;
    String resolution;
    String videoSizeString;
    boolean isBestVideo;
    List<List<String>> hashes;
    long numberOfKeyFrames;
    byte[] audioHashes;
}
