package vidupe.dedupe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;

@Data
@Builder
@Slf4j
public class DuplicateVideosList {
    private VideoHashesInformation referenceVideo;
    private List<VideoHashesInformation> duplicateVideosList;
    private HashMap<String,String> bestVideoIds;
    public String toJsonString() {
        log.info("Converting DuplicateVideosList class to json String");
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
