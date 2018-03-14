package vidupe.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class HashGenMessage {

    private String email;
    private String accessToken;
    private String videoId;
    private String videoName;
    private long videoSize;
    private long videoDuration;
    private long minHeight;
    private long minWidth;

    public byte[] toBytes() {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] value = null;
        try {
            value = objectMapper.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return value;
    }

    public String toJsonString() {
        ObjectMapper objectMapper = new ObjectMapper();
        String value = "";
        try {
            value = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return value;
    }
}
