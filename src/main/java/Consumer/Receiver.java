package Consumer;

import Schema.LiftRide;
import Schema.SkierLiftRide;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Receiver {

  private static Gson gson = new Gson();

  private static ConcurrentHashMap<String, Vector<LiftRide>> skierLiftRideMap = new ConcurrentHashMap<>();
  private final static String QUEUE_NAME = "skier_queue";
  private static final int NUM_THREADS = 35;
  private static final int ON_DEMAND = -1;
  private static final int WAIT_TIME_SECS = 1;
  private static final String SERVER = "localhost";
  private static JedisPool jedisPool;

  private static void recv(Connection conn) throws InterruptedException {

    for (int i=0; i < NUM_THREADS; i++) {
      Runnable thr = () -> {
        try {
          Channel channel = conn.createChannel();
          channel.queueDeclare(QUEUE_NAME, false, false, false, null);

          channel.basicQos(1);
          DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            // System.out.println(message);
            // receive a SkierLiftRide json
            SkierLiftRide skierLiftRide = gson.fromJson(message, SkierLiftRide.class);
            String skierId = skierLiftRide.getSkierId();
            int time = skierLiftRide.getTime(), liftID = skierLiftRide.getLiftID();

          /*      hashmap veresion
            if(skierLiftRideMap.containsKey(skierId)) {
              skierLiftRideMap.get(skierId).add(new LiftRide(time, liftID));
            } else {
              Vector<LiftRide> vs = new Vector<>();
              vs.add(new LiftRide(time, liftID));
              skierLiftRideMap.put(skierId, vs);
            }
            */
//            System.out.println(gson.toJson(skierLiftRide));
            // redis version
            Jedis jedis = null;
            try {
              jedis = jedisPool.getResource();
              jedis.lpush("skier"+skierId, gson.toJson(new LiftRide(time, liftID)));
              jedis.lpush("day"+time, skierId);
              // System.out.println("write to redis: " + skierLiftRide);
            } catch(Exception e) {
              e.printStackTrace();
            } finally {
              if(null != jedis) {
                jedis.close();
              }
            }

          };
          channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });

        } catch (IOException ex) {
          Logger.getLogger(Receiver.class.getName()).log(Level.INFO, null, ex);
        } catch (Exception ex) {
          Logger.getLogger(Receiver.class.getName()).log(Level.INFO, null, ex);
        }
      };
      new Thread(thr).start();
    }
  }

  public static void main(String[] argv) throws Exception {

    System.out.println("THREAD_NUM: " + NUM_THREADS);
    // System.out.println("Auto Ack");
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(SERVER);
    factory.setUsername("guest");
    factory.setPassword("guest");
    Connection connection = factory.newConnection();

    JedisPoolConfig jedisConfig = new JedisPoolConfig();
    jedisConfig.setMaxTotal(20);

    // local
    // jedisPool = new JedisPool(jedisConfig, "localhost", 6379, 100, "wzn990826");
    jedisPool = new JedisPool(jedisConfig, "localhost", 6379);
    System.out.println("INFO: RabbitMQ connection established");

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    recv(connection);
    // System.out.println(skierLiftRideMap.size());
  }

}
