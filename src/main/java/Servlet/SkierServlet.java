package Servlet;

import Constants.ServiceConstant;
import Consumer.RMQChannelFactory;
import Schema.LiftRide;
import Schema.ResponseMessage;
import Schema.SkierLiftRide;
import Schema.TotalVertival;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.concurrent.EventCountCircuitBreaker;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

@WebServlet
public class SkierServlet extends HttpServlet {
  private String requestQueueName = "skier_queue";
  private Gson gson = new Gson();

  private ObjectPool<Channel> channelPool;

  public void init() {
    this.channelPool = new GenericObjectPool<>(new RMQChannelFactory());
  }
  @Override
  protected void doGet(
      HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");

    String urlPath = req.getPathInfo();
    String[] urlParts = new String[0];
    if(urlPath!=null) {
      urlParts = urlPath.split("/");
    }
    String service = getUrlType(urlParts);
    ResponseMessage msg;
    switch (service) {
      case ServiceConstant.GET_SKI_DAY_VERT: {
        PrintWriter out = res.getWriter();
        TotalVertival totalVertival = new TotalVertival(34507);
        String msgJson = this.gson.toJson(totalVertival);
        out.println(msgJson);
        res.setStatus(HttpServletResponse.SC_OK);
        break;
      }
      case ServiceConstant.GET_TOTAL_VERT: {
        PrintWriter out = res.getWriter();
        msg = new ResponseMessage("get the total vertical for the skier for specified seasons at the specified resort");
        String msgJson = this.gson.toJson(msg);
        out.println(msgJson);
        res.setStatus(HttpServletResponse.SC_OK);
        break;
      }
      default: {
        PrintWriter out = res.getWriter();
        msg = new ResponseMessage("No service found");
        String msgJson = this.gson.toJson(msg);
        out.println(msgJson);
        res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    }

  }

  private String getUrlType(String[] urlPath) {
    if(urlPath.length == 8
        && urlPath[2].equals("seasons")
        && urlPath[4].equals("days")
        && urlPath[6].equals("skiers")) {
      return ServiceConstant.GET_SKI_DAY_VERT;
    } else if(urlPath.length == 3
        && urlPath[2].equals("vertical")) {
      return ServiceConstant.GET_TOTAL_VERT;
    }
    return ServiceConstant.SERVICE_NOT_FOUND;
  }

  private String postUrlType(String[] urlPath) {
    if(urlPath.length == 3 && urlPath[2].equals("seasons")) {
      return ServiceConstant.WRITE_LIFT_TO_SKIER;
    }
    return ServiceConstant.SERVICE_NOT_FOUND;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();
    String[] urlParts = urlPath.split("/");
    String resortId = urlParts[1];
    String seasonId = urlParts[3];
    String dayId = urlParts[5];
    String skierId = urlParts[7];
    if(urlParts.length <= 1
        || !isInteger(resortId)
        || !isInteger(seasonId)
        || !isInteger(dayId)
        || !isInteger(skierId)) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMessage msg = new ResponseMessage("Invalid inputs");
      PrintWriter out = res.getWriter();
      String msgJson = this.gson.toJson(msg);
      out.println(msgJson);
      return;
    }

    if(!findResortById(resortId)
        || !findSeasonById(seasonId)
        || !findDayById(dayId)
        || !findSkierById(skierId)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMessage msg = new ResponseMessage("Data Not Found");
      PrintWriter out = res.getWriter();
      String msgJson = this.gson.toJson(msg);
      out.println(msgJson);
      return;
    }

    StringBuilder sb = new StringBuilder();
    String s;
    while ((s = req.getReader().readLine()) != null) {
      sb.append(s);
    }
    LiftRide json = (LiftRide) gson.fromJson(sb.toString(), LiftRide.class);
    int time = Integer.valueOf(json.getTime());
    int liftId = Integer.valueOf(json.getLiftID());
    if(liftId<1 || liftId>40 || time<1 || time>360) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMessage msg = new ResponseMessage("Invalid inputs");
      PrintWriter out = res.getWriter();
      String msgJson = this.gson.toJson(msg);
      out.println(msgJson);
    }

    // send to queue
    LiftRide liftRide = new LiftRide(time, liftId);
    SkierLiftRide skierLiftRide = new SkierLiftRide(skierId, liftRide);
    String msg = gson.toJson(skierLiftRide);
    publishRMQ(msg);

    res.setStatus(HttpServletResponse.SC_CREATED);
  }

  public void publishRMQ(String msg) {
    Channel channel = null;
    try {
      channel = channelPool.borrowObject();
      channel.queueDeclare(requestQueueName, false, false, false, null);
      channel.basicPublish("", requestQueueName, null, msg.getBytes());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        channelPool.returnObject(channel);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private boolean findResortById(String resortId) {
    return true;
  }
  private boolean findSeasonById(String seasonId) {
    return true;
  }
  private boolean findDayById(String dayId) {
    return true;
  }
  private boolean findSkierById(String skierId) {
    return true;
  }

  private boolean isInteger(String s) {
    try{
      int num = Integer.parseInt(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private void sendRMQ() {

  }
}

