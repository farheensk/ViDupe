package vidupe.dedupe;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.junit.Test;
import vidupe.constants.Constants;

public class VidupeStoreManagerTest {
    private Datastore datastore;
    private VidupeStoreManager vidupeStoreManager;

    public VidupeStoreManagerTest() {
        this.datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        this.vidupeStoreManager = new VidupeStoreManager(datastore);
    }


    @Test
    public void retrieveHashes() {
    }

    @Test
    public void retrieveEntityInformation() {
    }

    @Test
    public void getVideoIdsOfUser() {
    }

    @Test
    public void getVideoHashesFromStore() {
    }


}