package vidupe.dedupe;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class VideoHashesInformation {
    String videoID;
    String videoName;
    long duration;
    List<List<String>> hashes;

}
