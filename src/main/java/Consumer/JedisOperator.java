package Consumer;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Jedis;

public class JedisOperator {
  private Jedis jedis;
  public JedisOperator(Jedis jedis) {
    this.jedis = jedis;
  }

  public int getSkiDaysForSeason(int skierId) {
    return (int) jedis.llen("skier"+skierId);
  }

  public int getVerticalTotalsForSkiDay(int skierId) {
    return -1;
  }

  public List<String> getLiftsForSkiDays(int time) {
    List<String> list = jedis.mget("day"+time);
    return list;
  }

}
