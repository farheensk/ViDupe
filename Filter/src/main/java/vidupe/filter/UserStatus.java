package vidupe.filter;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UserStatus {
    private String jobId;
    private String email;
    private int totalVideos;
    private int filteredVideos;
    private boolean phashgenProcessed;
    private boolean dedupeProcessed;
}
