package com.smarttarget.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.scalactic.Bool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by luo on 12/10/16.
 */
public class WikiLogParse {

    static Function2<Row,Row,Row> sum = (Row r1, Row r2) -> {
        Long newRow[] = new Long[2];
        for( int i = 0; i < 2; i++ )
            newRow[i] = (Long)r1.get(i) + (Long)r2.get(i);
        return RowFactory.create(newRow);
    };

    static Function<String, Boolean> filter = (Function<String, Boolean>) line -> {
        String words[] = line.split(" ");
        if( words.length == 4 )
            return true;
        else
            return false;
    };


    public static void main( String[] args )
    {
        String fname = args[0];
        Boolean tempfile = false;
        Utils.append = true;
        Utils.init();
        SparkConf conf = new SparkConf().setAppName("WikiLogParse");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /* Prep File
        if( fname.contains("gz") ) {
            try {
                // Execute command
                String tmp[]= fname.split( System.getProperty("file.separator") );
                String newfname = tmp[tmp.length-1] + ".txt";
                String command = "gunzip -c " + fname + " > " + newfname;
                fname = newfname;
                System.out.println( "COMMAND:" + command );
                tempfile = true;
                Process child = Runtime.getRuntime().exec( command );
//                child.waitFor();
            } catch (Exception e) {
                System.out.println( "ERROR:" + e.getMessage() );
                e.printStackTrace();
            }
        }
        */
        JavaRDD<String> textFile = sc.textFile( fname );
        JavaRDD<String> textFileFiltered  = textFile.filter(filter);

        JavaRDD<Row> rowRDD = textFileFiltered.map(
                new Function<String, Row>() {
                    public Row call(String line) throws Exception {
                        String words[] = line.split(" ");
                        Long data[] = new Long[2];
                        data[0] = Long.valueOf( words[2] );
                        data[1] = Long.valueOf( words[3] );
                        return RowFactory.create(data);
                    }
                });

        Row results = rowRDD.reduce(sum);
        System.out.println("DONE:" + fname + " " + results.getLong(0) + " " + results.getLong(1) );

        Utils.log( fname + " " + results.getLong(0) + " " + results.getLong(1) );
        Utils.fini();

        if( tempfile ) {
            try {
                // Execute command
                //String command = "rm " + fname;
                //Process child = Runtime.getRuntime().exec(command);
                //child.wait();
            } catch (Exception e) {
            }
        }

    }
}
