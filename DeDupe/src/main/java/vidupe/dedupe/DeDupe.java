package vidupe.dedupe;


import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.SubscriptionName;
import vidupe.constants.Constants;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/dedupe")
public class DeDupe extends HttpServlet {

    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {
            receiveMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public StringBuilder receiveMessages() throws InterruptedException {
        SubscriptionName subscription = SubscriptionName.of(Constants.PROJECT, Constants.SUBSCRIPTION);
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService());
        MessageReceiver receiver = new VidupeMessageProcessor(vidupeStoreManager);
        Subscriber subscriber = Subscriber.newBuilder(subscription, receiver).build();
        subscriber.addListener(
                new Subscriber.Listener() {
                    @Override
                    public void failed(Subscriber.State from, Throwable failure) {
                        // Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
                        System.err.println(failure);
                    }
                },
                MoreExecutors.directExecutor());
        subscriber.startAsync().awaitRunning();
        StringBuilder display = new StringBuilder("default");
        return display;
    }
}
