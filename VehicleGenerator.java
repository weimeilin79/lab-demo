import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;

public class VehicleGenerator extends RouteBuilder {

    private static int MAX_POOL_SIZE = 20;

    private AtomicLong vehicleIds = new AtomicLong();
    private ConcurrentLinkedQueue<VehicleInfo> activeVehicles = new ConcurrentLinkedQueue<>();

    public void configure() {

        from("timer:tick?period=5s")
                .choice()
                    .when(is -> activeVehicles.size() < MAX_POOL_SIZE)
                        .to("direct:generate")
                    .otherwise()
                        .to("direct:remove");

        from("direct:generate")
                .setBody(this::randomVehicle)
                .process(e -> activeVehicles.add(e.getMessage().getBody(VehicleInfo.class)))
                .loadBalance().roundRobin()
                    .to("direct:sendUBER")
                    .to("direct:sendLYFT")
                .end();

        from("direct:remove")
                .setBody(() -> activeVehicles.poll())
                .process(e -> e.getMessage().getBody(VehicleInfo.class).setAvailable(false))
                .recipientList(simple("direct:send${body.getProvider()}"));


        from("direct:sendUBER")
                .process(e -> e.getMessage().getBody(VehicleInfo.class).setProvider("UBER"))
                .marshal().json(JsonLibrary.Jackson)
                .to("kafka:uber?brokers=my-cluster-kafka-bootstrap.streams.svc:9092");

        from("direct:sendLYFT")
                .process(e -> e.getMessage().getBody(VehicleInfo.class).setProvider("LYFT"))
                .setBody(simple("${body.getVehicleId()},${body.getPricePerMinute()},${body.getTimeToPickup()},${body.getAvailableSpace()},${body.isAvailable()}"))
                .to("kafka:lyft?brokers=my-cluster-kafka-bootstrap.streams.svc:9092");

    }

    public VehicleInfo randomVehicle() {
        VehicleInfo info = new VehicleInfo();
        info.setAvailableSpace(2 + (int)(Math.random() * 7)); // 2 - 8
        info.setPricePerMinute(1.0 + Math.random() * 9); // 1.0 - 9.99
        info.setTimeToPickup(1 + (long)(Math.random() * 21)); // 1 - 20
        info.setVehicleId(vehicleIds.incrementAndGet());
        info.setAvailable(true);

        return info;
    }

    public static class VehicleInfo {

        @JsonIgnore
        private String provider;

        private long vehicleId;

        private double pricePerMinute;

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
                    ", timeToPickup=" + timeToPickup +
                    ", availableSpace=" + availableSpace +
                    ", available=" + available +
                    '}';
        }
    }


}


