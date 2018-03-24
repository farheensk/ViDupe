package vidupe.dedupe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class DuplicateVideosList {
    private List<List<VideoHashesInformation>> duplicateVideosList;
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
