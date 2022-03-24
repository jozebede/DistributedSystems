
package edu.yu.cs.com3800;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {

    // has some default methods. Allows me to put my configurable logging logic in one place
    //Logger initializeLogging(String s, boolean b);

//    default Logger initializeLogging(String fileNamePreface, Boolean disableParentHandlers ){
//        initializeLogging(fileNamePreface, true);
//        return
//    }

    default Logger initializeLogging(String fileNamePreface ) {

        boolean disableParentHandlers = true;
        Logger logger = Logger.getLogger(fileNamePreface);
        //logger.setLevel(Level.FINER);


        FileHandler fh = null;
        try {
            fileNamePreface.replace("edu.yu.cs.com3800", "");
            fh = new FileHandler("./Log/LogFile_" + fileNamePreface  +".log");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //fh.setLevel(Level.FINER);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.setUseParentHandlers(disableParentHandlers);
        //fh.close();


        return logger;

    }
}











//package edu.yu.cs.com3800;
//
//import java.io.IOException;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.logging.FileHandler;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import java.util.logging.SimpleFormatter;
//
//public interface LoggingServer {
//
//    // has some default methods. Allows me to put my configurable logging logic in one place
//    //Logger initializeLogging(String s, boolean b);
//
//    default Logger initializeLogging(String fileNamePreface){
//       return this.initializeLogging(fileNamePreface,false);
//
//    }
//
//
//    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) {
//
//        Logger logger = Logger.getLogger(fileNamePreface);
//        FileHandler fh = null;
//
//
//        try {
//            SimpleFormatter formatter = new SimpleFormatter();
//            SimpleDateFormat format = new SimpleDateFormat("M-d_HHmmss");
//          //  fh = new FileHandler("./LogFile_" + fileNamePreface  +".log");
//            fh = new FileHandler("./LogFile_" + format.format(Calendar.getInstance().getTime()) + ".log");
//
//            //fh.setLevel(Level.FINER);
//            logger.addHandler(fh);
//
//            fh.setFormatter(formatter);
//            logger.setUseParentHandlers(disableParentHandlers);
//            //fh.close();
//
//
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//        logger.info("info msg");
//        logger.severe("error message");
//        logger.fine("fine message");
//
//        return logger;
//
//    }
//
//
//
//}
//
//
//
