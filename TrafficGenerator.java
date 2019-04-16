import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;

public class TrafficGenerator extends RouteBuilder {

    private static final long MIN_TIME = 10, MAX_TIME = 50;

    private static final String[] DESTINATIONS = {"AIRPORT"}; // {"AIRPORT", "STADIUM", "STATION"};
    private int currentDestination;
    private long lastCommunication = 20;

    public void configure() {

        from("timer:tick?period=6s")
                .setBody(method(this, "generateTraffic"))
                .marshal().json(JsonLibrary.Jackson)
                .to("seda:traffic");

    }

    public TrafficInfo generateTraffic() {
        TrafficInfo info = new TrafficInfo();
        int pos = this.currentDestination++ % DESTINATIONS.length;
        if (pos < 0) {
            pos = 0;
            this.currentDestination = 1;
        }
        long lastValue = this.lastCommunication;
        long delta = 3 - (long) (Math.random() * 7); // [-3 , +3]
        long newValue = lastValue + delta;
        newValue = Math.min(MAX_TIME, newValue);
        newValue = Math.max(MIN_TIME, newValue);
        this.lastCommunication = newValue;

        info.setDestination(DESTINATIONS[pos]);
        info.setExpectedTime(newValue);
        return info;
    }

    public static class TrafficInfo {

        private String destination;

        private long expectedTime;

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public long getExpectedTime() {
            return expectedTime;
        }

        public void setExpectedTime(long expectedTime) {
            this.expectedTime = expectedTime;
        }

        @Override
        public String toString() {
            return "TrafficInfo{" +
                    "destination='" + destination + '\'' +
                    ", expectedTime=" + expectedTime +
                    '}';
        }
    }


}


