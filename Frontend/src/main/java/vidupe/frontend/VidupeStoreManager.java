package vidupe.frontend;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vidupe.constants.EntityProperties;

public class VidupeStoreManager {
    private static final Logger logger = LoggerFactory.getLogger(VidupeStoreManager.class);
    private final Datastore datastore;

    public VidupeStoreManager(Datastore dataStore) {
        this.datastore = dataStore;
    }
    public void addAccessTokenToDataStore(String jobId, String email, String accessToken) {
        Key key = createTokenKey(jobId,email);
        Entity entity = Entity.newBuilder(key)
                .set("accessToken", accessToken).build();
        datastore.put(entity);

    }

    public Key createTokenKey(String jobId, String email) {
        Key key = datastore.newKeyFactory()
                .setKind("tokens")
                .addAncestors(PathElement.of("user", email))
                .newKey(jobId);
        return key;
    }


    public String createEntity(GoogleAccount data, String jobId) {

        Key key = createKey(jobId, data.getEmail());
        // key = datastore.newKeyFactory().setKind("users").newKey(data.getEmail());
        String ifExists = "false";
        Entity task = Entity.newBuilder(key)
                .set(EntityProperties.USER_ID,data.getId())
                .set(EntityProperties.NAME, data.getName())
                .set(EntityProperties.EMAIL_ID,data.getEmail())
                .set(EntityProperties.TOTAL_VIDEOS, -1)
                .set(EntityProperties.FILTERED_VIDEOS_COUNT,-1)
                .set(EntityProperties.PHASHGEN,false)
                .set(EntityProperties.DEDUPE, false)
                .set(EntityProperties.CREATED, Timestamp.now())
                .set(EntityProperties.DONE, false)
                .build();
        try {
            for(int i=0;i<3;i++){
                datastore.put(task);
                Entity newEntity = datastore.get(key);
                if(newEntity.getString(EntityProperties.EMAIL_ID).equals(data.getEmail()));
                   break;
            }

        } catch (DatastoreException ex) {
            if ("ALREADY_EXISTS".equals(ex.getReason())) {
                // entity.getKey() already exists
                ifExists = "true";
                resetEntityProperty(datastore, key, task, jobId);
            }
        }
        return ifExists;
    }
    public Key createKey(String keyName, String ancestorId) {
        Key key = datastore.newKeyFactory()
                .setKind("users")
                .addAncestors(PathElement.of("user", ancestorId))
                .newKey(keyName);
        return key;
    }

    private void resetEntityProperty(Datastore datastore, Key key, Entity task, String jobId) {
        Entity newtask = Entity.newBuilder(key)
                .set(EntityProperties.USER_ID,task.getString(EntityProperties.USER_ID))
                .set(EntityProperties.NAME, task.getString(EntityProperties.NAME))
                .set(EntityProperties.EMAIL_ID,task.getString(EntityProperties.EMAIL_ID))
                .set(EntityProperties.TOTAL_VIDEOS, -1)
                .set(EntityProperties.FILTERED_VIDEOS_COUNT,-1)
                .set(EntityProperties.PHASHGEN,false)
                .set(EntityProperties.DEDUPE, false)
                .set(EntityProperties.CREATED, task.getTimestamp(EntityProperties.CREATED))
                .set(EntityProperties.DONE, false)
                .build();
        datastore.put(newtask);

    }

    public boolean checkIfDeDupeModuleDone(String email, String jobId) {
        boolean ifDedupeProcessed = false;
        Key key = createKey(jobId, email);
        Entity entity = datastore.get(key);
        if(entity.getBoolean(EntityProperties.DEDUPE) == true)
            ifDedupeProcessed = true;
        return ifDedupeProcessed;
    }
    public String getAccessTokenOfUser(String jobId, String email) {
        Datastore datastore = DatastoreOptions.newBuilder().setNamespace("vidupe").build().getService();
        Key key = datastore.newKeyFactory()
                .setKind("tokens")
                .addAncestors(PathElement.of("user", email))
                .newKey(jobId);
        Entity entity = datastore.get(key);
        return entity.getString("accessToken");
    }
}
