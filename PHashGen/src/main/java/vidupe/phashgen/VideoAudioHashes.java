package vidupe.phashgen;

import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;

@Data
@Builder
public class VideoAudioHashes {
    ArrayList<String> videoHashes;
    byte[] audioHashes;
}
