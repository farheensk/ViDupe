package vidupe.phashgen;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.junit.Test;
import vidupe.constants.Constants;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class VidupeMessageProcessorTest {
    Datastore datastore;
    VidupeStoreManager vidupeStoreManager;
    VidupeMessageProcessor vidupeMessageProcessor;

    public VidupeMessageProcessorTest() {
        datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        vidupeStoreManager = new VidupeStoreManager(datastore);
        vidupeMessageProcessor = new VidupeMessageProcessor(vidupeStoreManager);

    }

    @Test
    public void convertStringHashesToLongTest() {
        final ArrayList<Long> longs = vidupeMessageProcessor.convertStringHashesToLong(null);
    }
}