package vidupe.dedupe;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@JsonIgnoreProperties("hashes")
public class VideoHashesInformation {
    String videoID;
    String videoName;
    long duration;
    List<List<String>> hashes;
    long numberOfKeyFrames;

}
