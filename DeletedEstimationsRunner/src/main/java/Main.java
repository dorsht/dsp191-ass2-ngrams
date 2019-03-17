
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Main {

    private static String bucketName, keyName, serviceRole, jobFlowRole;


    /** Assumption: The user used the userInfo file correctly - The first row is the bucket name, the second
     one is the queue name, the third one is the role arn, etc.
     **/
    private static void readUserInfo () throws IOException {
        File file = new File("./userInfo.txt");

        BufferedReader br = new BufferedReader(new FileReader(file));

        bucketName = br.readLine();
        keyName = br.readLine();
        serviceRole = br.readLine();
        jobFlowRole = br.readLine();

        br.close();
    }


    public static void main(String[] args) {

        try {
            readUserInfo();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BasicConfigurator.configure();

        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://vladi-dsp-191-ass2-bucket/DeletedEstimations.jar")
                .withMainClass("ass2.MainClass")
                .withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data",
                        "s3n://" + bucketName + "/deleted-estimations-output");

        StepConfig stepConfig = new StepConfig()
                .withName("Calculate_Deleted_Estimations")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName(keyName)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Deleted Estimations on hebrew 3Gram")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole(serviceRole)
                .withJobFlowRole(jobFlowRole)
                .withLogUri("s3n://" + bucketName + "/deleted-estimations-logs/")
                .withReleaseLabel("emr-4.2.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();

        System.out.println("Ran job flow with id: " + jobFlowId);
    }

}
