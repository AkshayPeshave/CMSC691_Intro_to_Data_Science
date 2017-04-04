/**
 * KMeans Clustering implemented using chained MR and MCR tasks
 * Author : Akshay Peshave (peshave1@umbc.edu)
 *
 *
 * INPUT: number of clusters, number of dimensions in data file, HDFS path to input file/folder, HDFS path to output
 * file/folder
 *
 *
 * OUTPUT:
 * 1. outputPath/oldCentroids.txt - contains final oldCentroids
 * 2. outputPath/iteration_*\/clusterAssignment  - first MR task output, contains cluster assignments for each
 *                                                  point for the iteration. Last iteration will have final
 *                                                  centroid assignments.
 * 3. outputPath/iteration_*\/centroidCompute  - second MCR task output, contains new clusters computed in
 *                                              the iteration. The last iteration will have final oldCentroids.
 */

import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import java.util.stream.Collectors;
import java.lang.Math;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {
    /**
     * KMeans Mapper: Assigns data points to nearest available centroid
     */
    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        public final static String centroidsFile = "centroids.txt";
        public ArrayList<ArrayList<Float>> centroids = new ArrayList<ArrayList<Float>>();

        /**
         * Setup mapper with oldCentroids computed in previous iteration (or initialised for first iteration)
         *
         * @param context
         * @throws IOException
         */
        public void setup(Context context) throws IOException {
            // Read oldCentroids from centroid.txt
            FileReader fileReader = new FileReader(centroidsFile);

            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                ArrayList<Float> centroid = new ArrayList<Float>();
                for (String val : line.split("[\\s,]")) {
                    centroid.add(Float.parseFloat(val));
                }
                centroids.add(centroid);
            }
            bufferedReader.close();
        }

        /**
         * Compute distance of a given data point from all oldCentroids and assign it to nearest centroid
         *
         * @param key
         * @param pointString
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text pointString, Context context
        ) throws IOException, InterruptedException {
            // read point in text form into a float array list
            ArrayList<Float> point = new ArrayList<Float>();
            for (String val : pointString.toString().split("[\\s,]")) {
                point.add(Float.parseFloat(val));
            }

            // iterate over all oldCentroids to find centroid nearest to the point
            float distance = 0;
            float minDistance = 999999999.9f;
            int nearestCentroid = -1;

            int k = centroids.size();
            int d = point.size();
            for (int i = 0; i < k; i++) {
                distance = 0;
                for (int j = 0; j < d; j++) {
                    distance += (point.get(j) - (centroids.get(i)).get(j)) * (point.get(j) - (centroids.get(i)).get(j));
                }
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCentroid = i;
                }
            }

            context.write(new IntWritable(nearestCentroid), pointString);
            //System.out.println("KMeans Mapper Completed.");
        }
    }

    /**
     * KMeans Reducer : Short circuit to write data from KMeans Mapper to HDFS to be read by Centroid Compute MR
     */
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        /**
         * Short circuit shuffled cluster points to HDFS
         *
         * @param clusterId
         * @param points
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(IntWritable clusterId, Iterable<Text> points, Context context)
                throws IOException, InterruptedException {

            for (Text pointText : points) {
                context.write(clusterId, pointText);
            }
            //System.out.println("KMeans Reducer Completed.");
        }
    }

    /**
     * Centroid Compute Mapper: Short circuit KMeans MR points-cluster assignment output after reformating to
     * Centroid Compute Combiner-Reducer
     */
    public static class CentroidComputeMapper extends Mapper<Object, Text, IntWritable, Text> {
        /**
         * Check for any empty lines and extraneous whitesapces in previous MR output and pass clusterId-point pair
         * to combiner-reducer
         *
         * @param key
         * @param pointText
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text pointText, Context context) throws IOException, InterruptedException {
            String[] splitText = pointText.toString().split("[\\s,]");
            if (splitText.length > 0) {
                IntWritable clusterId = new IntWritable(Integer.parseInt(splitText[0]));
                String pointSpecs = "";
                for (int i = 1; i < splitText.length; i++) {
                    pointSpecs += splitText[i];
                    if (i < splitText.length - 1) {
                        pointSpecs += " ";
                    }
                }
                context.write(clusterId, new Text(pointSpecs));
            }
            //System.out.println("Centroid Mapper Completed.");
        }
    }

    /**
     * Cnetroid Compute Combiner: Combines points assigned to various clusters as they become available from the mapper
     * to improve shuffle efficiency
     */
    public static class CentroidComputeCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        /**
         * Computes intermediate sums across dimensions for points assigned to various clusters per mapper as they
         * become available from the mapper
         *
         * @param clusterId
         * @param points
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(IntWritable clusterId, Iterable<Text> points, Context context) throws IOException, InterruptedException {
            // Compute interim sum for a part of cluster-points assignment
            int num = 0;
            ArrayList<Float> sum = new ArrayList<Float>();

            for (Text pointText : points) {
                ArrayList<Float> point = new ArrayList<Float>();
                for (String coordinate : pointText.toString().split("[\\s,]")) {
                    point.add(Float.parseFloat(coordinate));
                }
                if (sum.size() == 0) {
                    for (int i = 0; i < point.size(); i++) {
                        sum.add(new Float(0.0));
                    }
                }
                num++;
                for (int i = 0; i < sum.size(); i++) {
                    sum.set(i, sum.get(i) + point.get(i));
                }
            }

            // Prepare Text value of interim sum and count of points considered
            String preres = "";
            for (int i = 0; i < sum.size(); i++) {
                preres += String.valueOf(sum.get(i)) + " ";
            }
            preres += String.valueOf((float) num);

            Text result = new Text(preres);
            context.write(clusterId, result);

            //System.out.println("Centroid Combiner Completed.");
        }
    }

    /**
     * Centroid Compute Reducer: Compute new oldCentroids for the current iteration
     */
    public static class CentroidComputeReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public static enum Counter {
            CONVERGED
        }

        public ArrayList<ArrayList<Float>> newCentroids = new ArrayList<ArrayList<Float>>();
        public final static String centerfile = "centroids.txt";
        public ArrayList<ArrayList<Float>> oldCentroids = new ArrayList<ArrayList<Float>>();

        /**
         * Setup the reducer by reading centroids computed in previous iteration
         *
         * @param context
         * @throws IOException
         */
        public void setup(Context context) throws IOException {
            FileReader fileReader = new FileReader(centerfile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                ArrayList<Float> centroid = new ArrayList<Float>();
                for (String val : line.split("[\\s,]")) {
                    centroid.add(Float.parseFloat(val));
                }
                oldCentroids.add(centroid);
            }

            bufferedReader.close();
        }

        /**
         * Compute new oldCentroids using the combiner output
         *
         * @param clusterId
         * @param pointSums
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(IntWritable clusterId, Iterable<Text> pointSums, Context context) throws IOException, InterruptedException {
            // Compute new centroids using interim sums from combiner
            float num = 0;
            ArrayList<Float> newCentroid = new ArrayList<Float>();
            for (Text intermediateSumText : pointSums) {
                // separate interim sums across dimensions and the count of interim points considered for this sum
                String[] intermediateSum = intermediateSumText.toString().split("[\\s,]");
                if (intermediateSum.length - 1 == 0) continue;
                int i;
                if (newCentroid.size() == 0) {
                    for (i = 0; i < intermediateSum.length - 1; i++) {
                        newCentroid.add(new Float(0.0));
                    }
                }

                // upodate cluster points sum and count based on this interim sum
                for (i = 0; i < newCentroid.size(); i++) {
                    newCentroid.set(i, newCentroid.get(i) + Float.parseFloat(intermediateSum[i]));
                }
                num += Float.parseFloat(intermediateSum[i]);
            }

            // compute new cluster centroid
            for (int i = 0; i < newCentroid.size(); i++) {
                newCentroid.set(i, newCentroid.get(i) / num);
            }
            newCentroids.add(newCentroid);

            // prepare Text value of new centroid
            String preres = "";
            for (int i = 0; i < newCentroid.size(); i++) {
                preres += String.valueOf(newCentroid.get(i));
                if (i < newCentroid.size() - 1) preres += ", ";
            }
            Text result = new Text(preres);
            context.write(clusterId, result);

            // Check for convergence of current cluster centroid and update counter appropriately
            if (!converged(clusterId, newCentroid))
                context.getCounter(Counter.CONVERGED).increment(1);
            //System.out.println("Centroid Reducer Completed.");
        }

        /**
         * Checks for convergence to new cluster centroid with centroid from previous iteration.
         * Convergence threshold is <=0.1 euclidean distance
         *
         * @param clusterId
         * @param newCentroid
         * @return
         */
        public boolean converged(IntWritable clusterId, ArrayList<Float> newCentroid) {
            ArrayList<Float> centroid = oldCentroids.get(clusterId.get());

            Double distance = new Double(0.0);
            for (int coordinate = 0; coordinate < centroid.size(); coordinate++) {
                distance += (centroid.get(coordinate) - newCentroid.get(coordinate)) *
                        (centroid.get(coordinate) - newCentroid.get(coordinate));
            }
            distance = Math.sqrt(distance);
            return distance <= 0.1 ? true : false;
        }

        /**
         * Cleanup method to update centroids.txt with oldCentroids computed in current iteration.
         *
         * @param context
         * @throws IOException
         */
        public void cleanup(Context context) throws IOException {
            //Create input stream from stringbuilder instance to write new centroids to HDFS file
            int k = newCentroids.size();
            int d = newCentroids.get(0).size();
            StringBuilder kCentroidsString = new StringBuilder();
            int i = -1, j = -1;
            for (ArrayList<Float> centroid : newCentroids) {
                i++;
                j = -1;
                for (Float coordinate : centroid) {
                    j++;
                    kCentroidsString.append(String.valueOf(coordinate));

                    if (j < d - 1) kCentroidsString.append(' ');
                }
                if (i < k - 1) kCentroidsString.append('\n');
            }
            InputStream in = new ByteArrayInputStream(kCentroidsString.toString().getBytes());

            //Update centroids.txt on HDFS
            Configuration conf = context.getConfiguration();
            String dst = "/user/hduser/output/kmeans/centroids.txt";
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            OutputStream out = fs.create(new Path(dst));
            IOUtils.copyBytes(in, out, 4096, true);
        }
    }

    /**
     * Initializes oldCentroids as <i,i,i.... for d dimensions></i,i,i....>
     *
     * @param k : number of clusters
     * @param d : number of dimensions
     * @throws Exception
     */
    static void initializeCentroids(Integer k, Integer d) throws Exception {
        //Create input stream from stringbuilder instance to write new oldCentroids to HDFS file
        StringBuilder kCentroidsString = new StringBuilder();
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < d; j++) {
                kCentroidsString.append(String.valueOf((float) i + 1));
                if (j < d - 1) kCentroidsString.append(' ');
            }
            if (i < k - 1) kCentroidsString.append('\n');
        }
        InputStream in = new ByteArrayInputStream(kCentroidsString.toString().getBytes());

        //Write oldCentroids.txt to HDFS
        Configuration conf = new Configuration();
        String dst = "/user/hduser/output/kmeans/centroids.txt";
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * Driver method for KMeans MR-MCR
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Logger.getRootLogger().setLevel(Level.WARN);
        // Validate if command line arguments passed adhere to requirements i.e. the necessary 4 arguments
        if (args.length != 4) {
            System.err.println("Usage: KMeans <#Clusters> <#Dimensions> <InputHDFSPath> <OutputHDFSPath>");
            System.out.println("Number of arguments provided = " + String.valueOf(args.length));
            System.exit(2);
        }

        // initalize oldCentroids
        initializeCentroids(Integer.parseInt(args[0]), Integer.parseInt(args[1]));

        // iteratively call MR-MCR chained tasks till oldCentroids convergence
        Integer iteration = 1;
        ArrayList<Long> convergenceSeq = new ArrayList<Long>();
        long counter = 1;
        convergenceSeq.add(counter);
        Integer previousIteration;
        while (counter > 0) {
            // Define KMeans cluster assignment job
            Configuration conf1 = new Configuration();
            Job clusterAssignmentJob = Job.getInstance(conf1, "KMeans");
            Path toCache1 = new Path("/user/hduser/output/kmeans/centroids.txt");
            clusterAssignmentJob.addCacheFile(toCache1.toUri());
            clusterAssignmentJob.setJarByClass(KMeans.class);
            clusterAssignmentJob.setMapperClass(KMeansMapper.class);
            clusterAssignmentJob.setReducerClass(KMeansReducer.class);
            FileInputFormat.addInputPath(clusterAssignmentJob, new Path(args[2]));
            clusterAssignmentJob.setMapOutputKeyClass(IntWritable.class);
            clusterAssignmentJob.setMapOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(clusterAssignmentJob, new Path(args[3] + "/iteration_" + iteration.toString() + "/clusterAssignment"));
            clusterAssignmentJob.setOutputKeyClass(IntWritable.class);
            clusterAssignmentJob.setOutputValueClass(Text.class);
            ControlledJob clusterAssignmentCJob = new ControlledJob(conf1);
            clusterAssignmentCJob.setJob(clusterAssignmentJob);

            // Define KMeans centroid compute job
            Configuration conf2 = new Configuration();
            Job centroidComputeJob = Job.getInstance(conf2, "KMeans");
            Path toCache2 = new Path("/user/hduser/output/kmeans/centroids.txt");
            centroidComputeJob.addCacheFile(toCache2.toUri());
            centroidComputeJob.setJarByClass(KMeans.class);
            centroidComputeJob.setMapperClass(CentroidComputeMapper.class);
            centroidComputeJob.setCombinerClass(CentroidComputeCombiner.class);
            centroidComputeJob.setReducerClass(CentroidComputeReducer.class);
            FileInputFormat.addInputPath(centroidComputeJob, new Path(args[3] + "/iteration_" + iteration.toString() + "/clusterAssignment"));
            centroidComputeJob.setMapOutputKeyClass(IntWritable.class);
            centroidComputeJob.setMapOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(centroidComputeJob, new Path(args[3] + "/iteration_" + iteration.toString() + "/centroidCompute"));
            centroidComputeJob.setOutputKeyClass(IntWritable.class);
            centroidComputeJob.setOutputValueClass(Text.class);
            ControlledJob centroidComputeCJob = new ControlledJob(conf2);
            centroidComputeCJob.setJob(centroidComputeJob);

            // chain the cluster assignment and centroid computation tasks and add to job controller
            centroidComputeCJob.addDependingJob(clusterAssignmentCJob);
            JobControl jobCtrl = new JobControl("jobCtrl");
            jobCtrl.addJob(clusterAssignmentCJob);
            jobCtrl.addJob(centroidComputeCJob);

            // start job controller in new thread
            Thread thread = new Thread(jobCtrl);
            thread.start();
            // wait for all chained jobs to complete
            while (!jobCtrl.allFinished()) {
                Thread.sleep(100);
            }

            // retrieve convergence counter from centroid computation job and append to convergence status sequence
            counter = centroidComputeJob.getCounters().findCounter(CentroidComputeReducer.Counter.CONVERGED).getValue();
            convergenceSeq.add(counter);

            iteration++;
        }

        // display convergence progress and exit
        System.out.println("Convergence after " + String.valueOf(iteration - 1) +
                " iterations. \nDeviating oldCentroids convergence progression displayed below.");
        System.out.println(convergenceSeq.subList(1, convergenceSeq.size()));
        System.exit(0);
    }
}
