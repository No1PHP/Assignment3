package Consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class RMQChannelFactory extends BasePooledObjectFactory<Channel> {

  private final Connection connection;
  private int count;

  public RMQChannelFactory(Connection connection) {
    this.connection = connection;
    count = 0;
  }

  public RMQChannelFactory() {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setUsername("guest");
    factory.setPassword("guest");
    try {
      connection = factory.newConnection();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  synchronized public Channel create() throws IOException {
    count ++;
    Channel chan = connection.createChannel();
    return chan;

  }

  @Override
  public PooledObject<Channel> wrap(Channel channel) {
    return new DefaultPooledObject<>(channel);
  }

  public int getChannelCount() {
    return count;
  }

}
