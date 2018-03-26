package vidupe.filter;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DurationFilterTest {

    @Test
    public void filterOutDurations() {
        DurationFilter durationFilter = new DurationFilter();
        VideoMetaData video1 = VideoMetaData.builder().duration(100).build();
        VideoMetaData video2 = VideoMetaData.builder().duration(200).build();
        VideoMetaData video3 = VideoMetaData.builder().duration(30).build();
        VideoMetaData video4 = VideoMetaData.builder().duration(40).build();
        List<VideoMetaData> files = new ArrayList<>();
        files.add(video1);
        files.add(video2);
        files.add(video3);
        files.add(video4);
        durationFilter.filterOutDurations(files);
    }

}