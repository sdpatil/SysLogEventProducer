package com.spnotes.sparkstreaming;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;

import java.io.IOException;
import java.net.*;

/**
 * Created by gpzpati on 10/29/15.
 */
public class SysLogEventProducer {
    private static final Logger logger = Logger.getLogger(SysLogEventProducer.class);
    final int SYSLOG_PORT = 514;
    static String syslogHost;

    public static void main(String[] argv) throws Exception {
        if (argv.length != 3) {
            System.out.println();
        }
        String hostName = argv[0];
        int portNumber = Integer.parseInt(argv[1]);
        String logMessage = argv[2];
        SysLogEventProducer sysLogEventProducer = new SysLogEventProducer(hostName, portNumber);
        sysLogEventProducer.write(logMessage);
    }

    void write(String string) throws IOException {
        byte[] bytes = string.getBytes();
        DatagramPacket packet = new DatagramPacket(
                bytes, bytes.length,
                address, port);

        if (this.ds != null) {
            logger.debug("Sending packet " + packet);
            ds.send(packet);
        }

    }

    private InetAddress address;
    private DatagramSocket ds;
    private int port;

    public SysLogEventProducer(String syslogHost, int p) {
        logger.debug("Entering SysLogEventProducer");
        this.syslogHost = syslogHost;
        this.port = p;
        try {
            this.address = InetAddress.getByName(syslogHost);
        } catch (UnknownHostException e) {
            LogLog.error("Could not find " + syslogHost +
                    ". All logging will FAIL.", e);
        }

        try {
            this.ds = new DatagramSocket();
        } catch (SocketException e) {
            e.printStackTrace();
            LogLog.error("Could not instantiate DatagramSocket to " + syslogHost +
                    ". All logging will FAIL.", e);
        }
        logger.debug("Exiting SysLogEventProducer " + ds);

    }
}
