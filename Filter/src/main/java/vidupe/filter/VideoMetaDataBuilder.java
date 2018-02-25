package vidupe.filter;

import com.google.api.client.util.DateTime;

public class VideoMetaDataBuilder {


    private String name;
    private String id;
    private String description;
    private DateTime dateModified;
    private long videoSize;
    private long duration;
    private long height;
    private long width;

    public VideoMetaDataBuilder() {
    }

    public VideoMetaDataBuilder name(String name) {
        this.name = name;
        return this;
    }

    public VideoMetaDataBuilder id(String id) {
        this.id = id;
        return this;
    }

    public VideoMetaDataBuilder description(String description) {
        this.description = description;
        return this;
    }

    public VideoMetaDataBuilder dateModified(DateTime dateModified){
        this.dateModified = dateModified;
        return this;
    }

    public VideoMetaDataBuilder videoSize(long videoSize) {
        this.videoSize = videoSize;
        return this;
    }

    public VideoMetaDataBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }
    public VideoMetaDataBuilder height(long height) {
        this.height = height;
        return this;
    }

    public VideoMetaDataBuilder width(long width) {
        this.width = width;
        return this;
    }

    public VideoMetaData build() {
        VideoMetaData videoMetaData = new VideoMetaData(id, name, description, dateModified, videoSize, duration, height, width);
        this.clear();
        return videoMetaData;
    }

    private void clear() {
        this.id = "";
        this.name = "";
        this.description = "";
        this.videoSize = 0;
        this.duration = 0;
        this.width = 0;
        this.height = 0;
    }


}

