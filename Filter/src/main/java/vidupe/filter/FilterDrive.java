package vidupe.filter;

import com.google.api.client.util.DateTime;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.SubscriptionName;
import vidupe.filter.constants.Constants;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

@WebServlet("/filter")
public class FilterDrive extends HttpServlet {
    public List<VideoMetaData> listFiles;
    private static VideoMetaData getMetaData(VideoMetaDataBuilder builder, String id, String name, String description, DateTime dateModified, long size, long duration, long height, long width) {
        return builder.id(id).name(name).description(description).dateModified(dateModified).videoSize(size).duration(duration).height(height).width(width).build();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {

            List<VideoMetaData> list = receiveMessages();
            response.getWriter().print(list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<VideoMetaData> receiveMessages() {
        SubscriptionName subscription = SubscriptionName.of(Constants.PROJECT, Constants.SUBSCRIPTION);
        StringBuilder display = new StringBuilder("default");
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

        return VidupeMessageProcessor.list;

    }


}

