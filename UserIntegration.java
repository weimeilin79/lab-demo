import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;

public class UserIntegration extends RouteBuilder {

    private AtomicLong traffic = new AtomicLong();

    public void configure() throws Exception {

        from("kafka:traffic?brokers=my-cluster-kafka-bootstrap.streams.svc:9092")
                .unmarshal().json(JsonLibrary.Jackson, TrafficInfo.class)
                .filter(simple("${body.getDestination()} == 'AIRPORT'"))
                .process(e -> traffic.set(e.getMessage().getBody(TrafficInfo.class).getExpectedTime()))
                .log("Updated traffic: ${body}");


        from("kafka:lyft?brokers=my-cluster-kafka-bootstrap.streams.svc:9092")
                .unmarshal().csv().split().body()
                .setBody(method(this, "buildLyft(${body[0]}, ${body[1]}, ${body[2]}, ${body[3]}, ${body[4]})"))
                .to("direct:process");

        from("kafka:uber?brokers=my-cluster-kafka-bootstrap.streams.svc:9092")
                .unmarshal().json(JsonLibrary.Jackson, VehicleInfo.class)
                .process(e -> e.getMessage().getBody(VehicleInfo.class).setProvider("UBER"))
                .to("direct:process");


        from("direct:process")
                .process(e -> {
                    VehicleInfo v = e.getMessage().getBody(VehicleInfo.class);
                    v.setPrice(v.getPricePerMinute() * traffic.get());
                })
                .marshal().json(JsonLibrary.Jackson)
                .to("kafka:stream?brokers=my-cluster-kafka-bootstrap.streams.svc:9092");

        // TODO remove when setting Kafka
        from("kafka:stream?brokers=my-cluster-kafka-bootstrap.streams.svc:9092")
                .log("Processed: ${body}");
    }

    public static VehicleInfo buildLyft(long vehicleId, double pricePerMinute, long timeToPickup, int availableSpace, boolean available) {
        VehicleInfo v = new VehicleInfo();
        v.setProvider("LYFT");
        v.setVehicleId(vehicleId);
        v.setPricePerMinute(pricePerMinute);
        v.setTimeToPickup(timeToPickup);
        v.setAvailableSpace(availableSpace);
        v.setAvailable(available);

        return v;
    }

    public static class VehicleInfo {

        private String provider;

        private long vehicleId;

        private double pricePerMinute;

        private double price;

        private long timeToPickup;

        private int availableSpace;

        private boolean available;

        public String getProvider() {
            return provider;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public long getVehicleId() {
            return vehicleId;
        }

        public void setVehicleId(long vehicleId) {
            this.vehicleId = vehicleId;
        }

        public double getPricePerMinute() {
            return pricePerMinute;
        }

        public void setPricePerMinute(double pricePerMinute) {
            this.pricePerMinute = pricePerMinute;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public long getTimeToPickup() {
            return timeToPickup;
        }

        public void setTimeToPickup(long timeToPickup) {
            this.timeToPickup = timeToPickup;
        }

        public int getAvailableSpace() {
            return availableSpace;
        }

        public void setAvailableSpace(int availableSpace) {
            this.availableSpace = availableSpace;
        }

        public boolean isAvailable() {
            return available;
        }

        public void setAvailable(boolean available) {
            this.available = available;
        }

        @Override
        public String toString() {
            return "VehicleInfo{" +
                    "provider='" + provider + '\'' +
                    ", vehicleId=" + vehicleId +
                    ", pricePerMinute=" + pricePerMinute +
                    ", price=" + price +
                    ", timeToPickup=" + timeToPickup +
                    ", availableSpace=" + availableSpace +
                    ", available=" + available +
                    '}';
        }
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
