package vidupe.ffmpeg.phash;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class VideoHashesInformation {
    String videoName;
    float duration;
    List<List<String>> hashes;
    List<List<Long>> hashes1;

}
