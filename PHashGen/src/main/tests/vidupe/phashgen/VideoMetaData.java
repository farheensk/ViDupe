package vidupe.phashgen;

import com.google.api.client.util.DateTime;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class VideoMetaData {
    private String id;
    private String name;
    private String description;
    private DateTime dateModified;
    private long videoSize;
    private long duration;
    private long height;
    private long width;
}
