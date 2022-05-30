package producer;

import com.google.gson.Gson;
import jakarta.ws.rs.Path;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

@Path("/send")
public class LocationProducer {
    @Resource(mappedName = "jms/locationConnectionFactory")
    private ConnectionFactory connectionFactory;
    @Resource(mappedName = "jms/locationQueue")
    private Queue queue;
    private Connection sendingConn, receivingConn;
    private Session sendingSession, receivingSession;
    private Logger logger;
   
    public LocationProducer() {
       logger = Logger.getLogger(getClass().getName());
    }
   
    @PostConstruct
    public void setupJMSSessions()
    {
       if (connectionFactory == null)
       {
          logger.warning("Dependency injection of jms/locationQueue failed");
       } else {
          try {
             // obtain a connection to the JMS provider
             sendingConn = connectionFactory.createConnection();
             // obtain an untransacted context for producing messages
             sendingSession = sendingConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
             // obtain a connection to the JMS provider
             receivingConn = connectionFactory.createConnection();
             // obtain an untransacted context for producing messages
             receivingSession = receivingConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
             logger.info("Dependency injection of jms/locationQueue was successful");
          } catch (JMSException e) {
             logger.warning("Error while creating sessions: " + e);
          }
       }
       if (queue == null)
       {
          logger.warning("Dependency injection of jms/locationQueue failed");
       } else {
          logger.info("Dependency injection of jms/locationQueue was successful");
       }
    }
   
    @PreDestroy
    public void closeJMSSessions() {
       try {
          if (receivingSession != null)
             receivingSession.close();
          if (receivingConn != null)
             receivingConn.close();
          if (sendingSession != null)
             sendingSession.close();
          if (sendingConn != null)
             sendingConn.close();
       } catch (JMSException e) {
          logger.warning("Unable to close connection: " + e);
       }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
     public String sendMessage(String data) {
        CompletableFuture.runAsync(() -> {
            try {
                // obtain a producer of messages to send to the queue
                MessageProducer producer = sendingSession.createProducer(queue);
                // create and send a string message
                TextMessage message = sendingSession.createTextMessage();
                message.setText(data);
                // send messages to the queue
                logger.info("Send message to queue: " + message.getText());
                producer.send(message);
            } catch (Exception e) {
                logger.warning("Error while sending messages: " + e);
            }
        });
        SendLocationResponse res = new SendLocationResponse();
        res.setCode("200");
        res.setDescription("Success");
        return new Gson().toJson(res);
     }
}
