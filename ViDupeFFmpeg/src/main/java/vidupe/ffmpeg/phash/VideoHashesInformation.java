package vidupe.ffmpeg.phash;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class VideoHashesInformation {
    String videoName;
    long duration;
    List<List<String>> hashes;
    List<List<Long>> hashes1;
    long numberOfKeyFrames;
    byte[] audioHashes;

}
