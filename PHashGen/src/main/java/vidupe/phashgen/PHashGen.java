package vidupe.phashgen;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import vidupe.constants.Constants;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/phashgen")
public class PHashGen extends HttpServlet{


    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {
            receiveMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void receiveMessages() {
        ExecutorProvider executorProvider =
                InstantiatingExecutorProvider.newBuilder()
                        .setExecutorThreadCount(1)
                        .build();
        ProjectSubscriptionName subscription = ProjectSubscriptionName.of(Constants.PROJECT, Constants.SUBSCRIPTION);
        VidupeStoreManager vidupeStoreManager = new VidupeStoreManager(DatastoreOptions.newBuilder().setNamespace(Constants.NAMESPACE).build().getService());
        MessageReceiver receiver = new VidupeMessageProcessor(vidupeStoreManager);


        Subscriber subscriber =
                Subscriber.newBuilder(subscription, receiver)
                        .setExecutorProvider(executorProvider).build();
        subscriber.addListener(new Subscriber.Listener() {
            public void failed(Subscriber.State from, Throwable failure) {
                System.out.println(failure);
            }
        }, executorProvider.getExecutor());
        subscriber.startAsync();


//        Subscriber subscriber = Subscriber.newBuilder(subscription, receiver).build();
//        subscriber.addListener(
//                new Subscriber.Listener() {
//                    @Override
//                    public void failed(Subscriber.State from, Throwable failure) {
//                        // Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
//                        System.err.println(failure);
//                    }
//                },
//                MoreExecutors.directExecutor());
//        subscriber.startAsync().awaitRunning();
    }
}
