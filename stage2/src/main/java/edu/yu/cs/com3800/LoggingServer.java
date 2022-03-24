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

    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) {

        Logger logger = Logger.getLogger(fileNamePreface);
        //logger.setLevel(Level.FINER);


        FileHandler fh = null;
        try {
            fh = new FileHandler("./LogFile_" + fileNamePreface  +".log");
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



