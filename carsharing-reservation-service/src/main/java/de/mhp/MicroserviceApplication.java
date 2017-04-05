package de.mhp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.util.stream.Stream;

@EnableBinding(Sink.class)
@IntegrationComponentScan
@EnableDiscoveryClient
@SpringBootApplication
public class MicroserviceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MicroserviceApplication.class, args);
    }
}

@MessageEndpoint
class ReservationProcessor {

    private final ReservationRepository _reservationRepository;

    @Autowired
    public ReservationProcessor(ReservationRepository _reservationRepository) {
        this._reservationRepository = _reservationRepository;
    }

    @ServiceActivator(inputChannel = Sink.INPUT)
    public void acceptNewReservations(String reservationName){
        this._reservationRepository.save(new Reservation(reservationName));
    }
}

@Component
class DummyCommandLineRunner implements CommandLineRunner {

    private final ReservationRepository _repository;

    @Autowired
    public DummyCommandLineRunner(ReservationRepository repository){
        this._repository = repository;
    }

    @Override
    public void run(String... args) throws Exception {
        // List of all carsharing provider: http://www.carsharing-news.de/carsharing-anbieter/
        Stream.of("car2go", "DriveNow", "Flinkster","Cambio","Stadtmobil","teilAuto","Book N Drive", "Ford Carsharing")
                .forEach(n -> _repository.save(new Reservation(n)));

        _repository.findAll().forEach(System.out::println);
    }
}

@RestController
@RefreshScope
class MessageController{

    @Value("${message}")
    private String message;

    @RequestMapping("/message")
    public String read(){
        return this.message;
    }
}

@RepositoryRestResource
interface ReservationRepository extends JpaRepository<Reservation, Long> {
    // nobody kehrs about writing CRUD operations
}

@Entity
class Reservation {

    @Id
    @GeneratedValue
    private Long id;
    private String reservationName;

    public Reservation() {
    }

    public Reservation(String reservationName) {
        this.reservationName = reservationName;
    }

    @Override
    public String toString() {
        return "Reservation{" +
                "id=" + id +
                ", reservationName='" + reservationName + '\'' +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getReservationName() {
        return reservationName;
    }

    public void setReservationName(String reservationName) {
        this.reservationName = reservationName;
    }
}