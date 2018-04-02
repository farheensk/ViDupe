package vidupe.ffmpeg.phash;

import java.util.Comparator;

public class MapComparator implements Comparator<VideoHashesInformation>
{
    private final String key;

    public MapComparator(String key)
    {
        this.key = key;
    }

    public int compare(VideoHashesInformation first,
                       VideoHashesInformation second)
    {
        Long firstValue;
        Long secondValue;
        switch (key){
            case "numberOfKeyFrames":
//                firstValue = second.getDuration();
//                secondValue = first.getDuration();
                //returns in descending order

//                if(firstValue == secondValue){
//                    if(first.numberOfKeyFrames > second.numberOfKeyFrames){
                        firstValue = second.numberOfKeyFrames;
                        secondValue= first.numberOfKeyFrames;
//                    }
//                }

                break;
            default:
                firstValue = 0L;
                secondValue = 0L;
        }
        return firstValue.compareTo(secondValue);
    }
}