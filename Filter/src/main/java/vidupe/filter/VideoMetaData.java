package vidupe.filter;

import com.google.api.client.util.DateTime;

public class VideoMetaData {
    private String id;
    private String name;
    private String description;
    private DateTime dateModified;
    private long videoSize;
    private long duration;
    private long height;
    private long width;

    public VideoMetaData(String id, String name, String description, DateTime dateModified, long videoSize, long duration, long height, long width) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.dateModified = dateModified;
        this.videoSize = videoSize;
        this.duration = duration;
        this.height = height;
        this.width = width;
    }


    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public DateTime getDateModified(){ return dateModified;}

    public long getVideoSize() {
        return videoSize;
    }

    public long getDuration() {
        return duration;
    }

    public long getHeight() {
        return height;
    }

    public long getWidth() {
        return width;
    }
}
