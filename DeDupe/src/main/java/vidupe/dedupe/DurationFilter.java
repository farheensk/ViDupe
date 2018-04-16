package vidupe.dedupe;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static vidupe.constants.Constants.ALLOWED_OVERLAP;

@Slf4j
public class DurationFilter
{
    public List<VideoHashesInformation>  filterOutDurations(List<VideoHashesInformation> files)
    {
        log.info("Filtering user drive:Start");
        files.sort(new MapComparator("duration"));
        List<VideoHashesInformation> returnSet = new ArrayList<>();
        int visit[] = new int[files.size()];
        for(int i=0;i<(files.size()-1);i++){
            long longVideo = files.get(i).getDuration();
            if((longVideo - files.get(i+1).getDuration())<= longVideo*ALLOWED_OVERLAP){
                visit[i] = 1;
                visit[i+1]=1;
            }
        }
        for(int i=0;i<visit.length;i++){
            if(visit[i]==1){
                returnSet.add(files.get(i));
            }
        }
        log.info("Filtering user drive:End");
        return returnSet;
    }
}
