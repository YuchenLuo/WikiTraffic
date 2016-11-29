package com.smarttarget.spark;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;

public class Utils {
    public static boolean append = false;
    static PrintWriter writer;
    public static void log( String msg ){
        writer.println( msg );
        writer.flush();
    }

    public static void init() {
        init("log.txt");
    }
    public static void init( String fname ) {
        try {
            writer = new PrintWriter( new FileOutputStream( new File( fname ), append ) );
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    public static void fini() {
        writer.close();
    }
}
