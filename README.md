# Map Reduce program - Calculate the probability for Hebrew ngrams

================================================================================
Created by:
      Dor Shtarker & Vladimir Shargorodsky

================================================================================

This program can calculate the probability for Hebrew ngrams, in fact, We wrote this program for 3 grams, but It'll be able to work with any type of ngrams, if n > 1. (You can change It a bit and It'll support other languages)

================================================================================
Instructions:
1. Create S3 bucket in order to save the results.
2. Create Role with AmazonElasticMapReduceforEC2Role and AmazonElasticMapReduceRole in IAM.
3. Copy userInfo file into the folder where You'll run the program, then add the required details.
4. Compile the project in DeletedEstimationsRunner folder with the command mvn package.
5. Run It with java -jar DeletedEstimationsRunner-1.0.jar.
6. You'll see the results in YOUR_BUCKET_NAME/deleted-estimations-output and the logs in YOUR_BUCKET_NAME/deleted-estimations-logs.

================================================================================
