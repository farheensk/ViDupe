package vidupe.filter;

import com.google.api.client.util.DateTime;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.junit.After;
import org.junit.Test;
import vidupe.filter.constants.Constants;
import vidupe.message.FilterMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VidupeMessageProcessorTest {

    private static final String CLIENT_ID = "123456";
    private Datastore datastore;
    private VidupeStoreManager vidupeStoreManager;
    private List<String> keyList = new LinkedList<>();

    public VidupeMessageProcessorTest() {
        DatastoreOptions.Builder optionsBuilder = DatastoreOptions.newBuilder();
        DatastoreOptions datastoreOptions = optionsBuilder.setNamespace(Constants.NAMESPACE).build();
        this.datastore = datastoreOptions.getService();
        this.vidupeStoreManager = new VidupeStoreManager(datastore);
    }

    @Test
    public void shouldSendToHashGen() {
        vidupeStoreManager = new VidupeStoreManager(this.datastore);
        VidupeMessageProcessor vidupeMessageProcessor = new VidupeMessageProcessor(vidupeStoreManager);
        String id = getKey();
        DateTime dateModified = DateTime.parseRfc3339("2018-02-23T00:00:00Z");
        DateTime nextDateModified = DateTime.parseRfc3339("2018-02-23T10:00:00Z");
        VideoMetaData videoMetaData = createVideoMetaData(id, dateModified);
        assertTrue(vidupeMessageProcessor.sendToHashGen(CLIENT_ID, videoMetaData));
        videoMetaData = createVideoMetaData(id, nextDateModified);
        assertTrue(vidupeMessageProcessor.sendToHashGen(CLIENT_ID, videoMetaData));
        videoMetaData = createVideoMetaData(id, dateModified);
        assertFalse(vidupeMessageProcessor.sendToHashGen(CLIENT_ID, videoMetaData));
    }

    @Test
    public void shouldNotSendToHashGen() {
        vidupeStoreManager = new VidupeStoreManager(this.datastore);
        VidupeMessageProcessor vidupeMessageProcessor = new VidupeMessageProcessor(vidupeStoreManager);
        String id = getKey();
        DateTime dateModified = DateTime.parseRfc3339("2018-02-23T00:00:00Z");

        VideoMetaData videoMetaData = createVideoMetaData(id, dateModified);
        vidupeStoreManager.createEntity(videoMetaData, CLIENT_ID);
        assertFalse(vidupeMessageProcessor.sendToHashGen(CLIENT_ID, videoMetaData));
    }



    private VideoMetaData createVideoMetaData(String id, DateTime dateTime) {
        return VideoMetaData.builder().videoSize(100L).duration(10L).height(100L).dateModified(dateTime)
                .width(100L).id(id).description("crap").name("test-name").build();
    }

    private String getKey() {
        Random r = new Random();
        int k = r.nextInt(1000);
        String key = String.valueOf(k);
        keyList.add(key);
        return key;

    }

    @After
    public void cleanUp() {
        for(String k : keyList) {
            vidupeStoreManager.deleteEntity(k, CLIENT_ID);
        }
    }


    @Test
    public void filter() {
        Datastore datastore = DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService();
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(datastore);
        VidupeMessageProcessor vidupeMessageProcessor = new VidupeMessageProcessor(vidupeStoreManager);
        FilterMessage filterMessage = FilterMessage.builder().accessToken("ya29.GluDBTDk0hK2LidQqxoJsVCujoNFjts8EQ7jPlfoiBfBTmv5AM6DtGUDK1lJX8xMcgu7a79Qg_3qLhxGaGUYUk0bikqd9M3rhNUdMbUWJPDSv8sR5xaC_SWmIMaV").build();
        vidupeMessageProcessor.filter(filterMessage);

    }
}