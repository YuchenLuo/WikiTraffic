package com.yuchen.data;

import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ReduceMax implements ReduceFunction {
    @Override
    public Object call(Object o1, Object o2) throws Exception {
        if( o1 instanceof  Vector ) {
            Vector v1 = (Vector) ((Row) o1).get(0);
            Vector v2 = (Vector) ((Row) o2).get(0);
            double[] newRow = new double[v1.size()];
            for (int i = 0; i < v1.size(); i++) {
                newRow[i] = (v1.apply(i) > v2.apply(i)) ? v1.apply(i) : v2.apply(i);
            }
            return RowFactory.create(Vectors.dense(newRow));
        } else {
            Double v1 = (Double) ((GenericRow) o1).apply(0);
            Double v2 = (Double) ((GenericRow) o2).apply(0);
            return RowFactory.create( v1 > v2 ? v1:v2 );
        }
    }
}
class ReduceMin implements ReduceFunction {
    @Override
    public Object call(Object o1, Object o2) throws Exception {
        if( o1 instanceof  Vector ) {
            Vector v1 = (Vector) ((Row)o1).get(0);
            Vector v2 = (Vector) ((Row)o2).get(0);
            double[] newRow = new double[v1.size()];
            for( int i = 0; i < v1.size(); i++ ) {
                newRow[i] = ( v1.apply(i) > v2.apply(i) )? v2.apply(i) : v1.apply(i) ;
            }
            return RowFactory.create(Vectors.dense(newRow) );
        } else {
            Double v1 = (Double) ((GenericRow) o1).apply(0);
            Double v2 = (Double) ((GenericRow) o2).apply(0);
            return RowFactory.create( v1 > v2 ? v2:v1 );
        }
    }
}

public class RegressionModelBuilder {
    private static Logger log = LoggerFactory.getLogger( RegressionModelBuilder.class );

    public static void buildRegressionModel ( String fname ) {
        log.info( "RegressionModelBuilder " + fname );

        // Initialize spark connection
        SparkSession spark = SparkSession.builder().appName("regression-model-builder").getOrCreate();

        // Load data
        Dataset<Row> data = spark.read().format("libsvm").load( fname );

        // max and min values for normalized error calculation
        Dataset<Row> labelSet = data.select("label");
        Double max = ((Double)labelSet.reduce(new ReduceMax()).apply(0));
        Double min = ((Double)labelSet.reduce(new ReduceMin()).apply(0));

        // Setup indexers
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(data);

        // Split the data
        Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Setup regressor
        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("label")
                .setFeaturesCol("indexedFeatures");

        // Combine tasks into pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {featureIndexer, rf});

        // Execute training task on data and crete model
        PipelineModel model = pipeline.fit(trainingData);

        // Make some predictions on test data
        Dataset<Row> predictions = model.transform(testData);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        // Get performance metrics for predictions on test dataset
        double rmse = evaluator.evaluate(predictions);
        RandomForestRegressionModel rfModel = (RandomForestRegressionModel)(model.stages()[1]);
        log.info( "Normalized Root Mean Squared Error=" + rmse / (max - min) );

        // Save model to file
        log.info( "rfrm.txt", rfModel.toDebugString());
        try {
            model.save( fname+".rfrm");
        } catch (Exception e) {
            log.error("ERROR"+e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main( String[] args ) {
        // Load configuration
        String fname = "data.txt";
        if ( args.length > 0 )
            fname = args[0];
        buildRegressionModel( fname );
    }
}
