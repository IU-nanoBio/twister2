//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class KMeansJobMain {

  private static final Logger LOG = Logger.getLogger(KMeansJobMain.class.getName());

  public static void main(String[] args) throws ParseException {
    LOG.log(Level.INFO, "KMeans Clustering Job");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    Options options = new Options();
    options.addOption(KMeansConstants.ARGS_WORKERS, true, "workers");
    options.addOption(KMeansConstants.ARGS_ITR, true, "iter");
    options.addOption(KMeansConstants.ARGS_DIMENSIONS, true, "dim");

    options.addOption(KMeansConstants.ARGS_FNAME, true, "fname");
    options.addOption(KMeansConstants.ARGS_NUMBER_OF_POINTS, true, "points");
    options.addOption(KMeansConstants.ARGS_POINTS, true, "pointsfile");
    options.addOption(KMeansConstants.ARGS_CENTERS, true, "centersfile");
    options.addOption(KMeansConstants.ARGS_FILESYSTEM, true, "filesystem");

    options.addOption(KMeansConstants.ARGS_CLUSTERS, true, "clusters");
    options.addOption(KMeansConstants.ARGS_POINTS_SEED_VALUE, true, "pseedvalue");
    options.addOption(KMeansConstants.ARGS_CENTERS_SEED_VALUE, true, "cseedvalue");
    options.addOption(KMeansConstants.ARGS_DATA_INPUT, true, "generate");
    options.addOption(KMeansConstants.ARGS_PARALLELISM_VALUE, true, "4");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(options, args);

    String fileName = commandLine.getOptionValue(KMeansConstants.ARGS_FNAME);
    String datapointsFile = commandLine.getOptionValue(KMeansConstants.ARGS_POINTS);
    String centersFile = commandLine.getOptionValue(KMeansConstants.ARGS_CENTERS);
    String fileSystem = commandLine.getOptionValue(KMeansConstants.ARGS_FILESYSTEM);
    String dataInput = commandLine.getOptionValue(KMeansConstants.ARGS_DATA_INPUT);

    int numberOfPoints = Integer.parseInt(commandLine.getOptionValue(
            KMeansConstants.ARGS_NUMBER_OF_POINTS));
    int workers = Integer.parseInt(commandLine.getOptionValue(KMeansConstants.ARGS_WORKERS));
    int itr = Integer.parseInt(commandLine.getOptionValue(KMeansConstants.ARGS_ITR));
    int dim = Integer.parseInt(commandLine.getOptionValue(KMeansConstants.ARGS_DIMENSIONS));
    int numOfClusters = Integer.parseInt(commandLine.getOptionValue(KMeansConstants.ARGS_CLUSTERS));
    int pSeedValue =
            Integer.parseInt(commandLine.getOptionValue(KMeansConstants.ARGS_POINTS_SEED_VALUE));
    int cSeedValue =
            Integer.parseInt(commandLine.getOptionValue(KMeansConstants.ARGS_CENTERS_SEED_VALUE));
    int parallelismValue =
            Integer.parseInt(commandLine.getOptionValue(KMeansConstants.ARGS_PARALLELISM_VALUE));

    LOG.fine("workers:" + workers + "\titeration:" + itr + "\tdimension:" + dim
            + "\tnumber of clusters:" + numOfClusters + "\tfilename:" + fileName
            + "\tnumber of datapoints:" + numberOfPoints + "\tdatapoints file:" + datapointsFile
            + "\tcenters file:" + centersFile + "\tfilesys:" + fileSystem
            + "\tparllelism:" + parallelismValue);

    configurations.put(KMeansConstants.ARGS_FNAME, fileName);
    configurations.put(KMeansConstants.ARGS_POINTS, datapointsFile);
    configurations.put(KMeansConstants.ARGS_CENTERS, centersFile);
    configurations.put(KMeansConstants.ARGS_FILESYSTEM, fileSystem);
    configurations.put(KMeansConstants.ARGS_DATA_INPUT, dataInput);

    configurations.put(KMeansConstants.ARGS_NUMBER_OF_POINTS, Integer.toString(numberOfPoints));
    configurations.put(KMeansConstants.ARGS_WORKERS, Integer.toString(workers));
    configurations.put(KMeansConstants.ARGS_ITR, Integer.toString(itr));
    configurations.put(KMeansConstants.ARGS_DIMENSIONS, Integer.toString(dim));
    configurations.put(KMeansConstants.ARGS_CLUSTERS, Integer.toString(numOfClusters));
    configurations.put(KMeansConstants.ARGS_POINTS_SEED_VALUE, Integer.toString(pSeedValue));
    configurations.put(KMeansConstants.ARGS_CENTERS_SEED_VALUE, Integer.toString(cSeedValue));
    configurations.put(KMeansConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    int random = (int) (Math.random() * 10000);
    String jobName = "kmeans-job-" + random;

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName(jobName);
    jobBuilder.setWorkerClass(KMeansJob.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
