package vidupe.filter;

import java.util.Comparator;

public class MapComparator implements Comparator<VideoMetaData> {
    private final String key;

    public MapComparator(String key) {
        this.key = key;
    }

    public int compare(VideoMetaData first,
                       VideoMetaData second) {
        Long firstValue;
        Long secondValue;
        switch (key) {
            case "duration":
                firstValue = second.getDuration();
                secondValue = first.getDuration();
                //returns in descending order
                break;
            case "height":
                firstValue = first.getHeight();
                secondValue = second.getHeight();
                //returns in ascending order
                break;
            case "width":
                firstValue = first.getWidth();
                secondValue = second.getWidth();
                //returns in ascending order
                break;
            default:
                firstValue = 0L;
                secondValue = 0L;
        }
        return firstValue.compareTo(secondValue);
    }
}