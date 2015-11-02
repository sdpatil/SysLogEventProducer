package com.spnotes.sparkstreaming;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;

import java.io.IOException;
import java.net.*;

/**
 * Created by gpzpati on 10/29/15.
 */
public class SysLogEventProducer {
    private static final Logger logger =Logger.getLogger(SysLogEventProducer.class);
    final int SYSLOG_PORT = 514;
    static String syslogHost;

    public static void main(String[] argv)throws Exception{
      SysLogEventProducer sysLogEventProducer = new SysLogEventProducer("127.0.0.1",11111);
      for(int i = 0 ; i <10 ;i++)
          sysLogEventProducer.write("Sending message " + i);


    }

    void write(String string) throws IOException {
        byte[] bytes = string.getBytes();
        DatagramPacket packet = new DatagramPacket(
                bytes, bytes.length,
                address, port);

        if(this.ds != null) {
            logger.debug("Sending packet "+ packet);
            ds.send(packet);
        }

    }
    private InetAddress address;
    private DatagramSocket ds;
    private int port;

    public  SysLogEventProducer(String syslogHost, int p) {
        logger.debug("Entering SysLogEventProducer");
        this.syslogHost = syslogHost;
        this.port =p;
        try {
            this.address = InetAddress.getByName(syslogHost);
        }
        catch (UnknownHostException e) {
            LogLog.error("Could not find " + syslogHost +
                    ". All logging will FAIL.", e);
        }

        try {
            this.ds = new DatagramSocket();
        }
        catch (SocketException e) {
            e.printStackTrace();
            LogLog.error("Could not instantiate DatagramSocket to " + syslogHost +
                    ". All logging will FAIL.", e);
        }
        logger.debug("Exiting SysLogEventProducer " + ds);

    }
}
