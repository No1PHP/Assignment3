package Schema;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Zhining
 * @description
 * @create 2022-10-26-00-47
 **/
@Data
@AllArgsConstructor
public class SkierLiftRide {
  String skierId;
  int time;
  int liftID;
  public SkierLiftRide(String skierId, LiftRide liftRide) {
    this.skierId = skierId;
    this.time = liftRide.getTime();
    this.liftID = liftRide.getLiftID();
  }
}
