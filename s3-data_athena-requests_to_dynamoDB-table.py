import boto3
import csv
import time
import os
import logging
from urllib.parse import urlparse

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# These ENV are expected to be defined on the lambda itself:
# vpa_athena_database, vpa_ddb_table, vpa_metric_name, vpa_athena_query, region, vpa_s3_output_location

# Responds to lambda event trigger
def lambda_handler(event, context):
    athena_results =[]
    vpa_athena_querys = [os.environ['vpa_athena_total_count_query'],
        os.environ['vpa_athena_most_frequent_tweeter'], 
        os.environ['vpa_athena_total_count_tweets_last_week'],
        os.environ['vpa_athena_total_count_tweets_2_week']]
    for vpa_athena_query in vpa_athena_querys:
        athena_result = run_athena_query(vpa_athena_query, os.environ['vpa_athena_database'],
                                     os.environ['vpa_s3_output_location'])
        athena_results.append(athena_result)
    weekly_tweets_difference = str(round(-100 +(float(athena_results[2])/float(athena_results[3]))*100,2)) 
    upsert_into_DDB(str.upper(os.environ['vpa_count_metric']), athena_results[0], context)
    upsert_into_DDB(str.upper(os.environ['vpa_tweeter_metric']), athena_results[1], context)
    upsert_into_DDB(str.upper(os.environ['vpa_weekly_tweets_difference']), weekly_tweets_difference, context)
    logger.info("{0} reinvent tweets so far!".format(athena_results))
    return {'message': "{0} reinvent tweets so far!".format(athena_result)}


# Runs athena query, open results file at specific s3 location and returns result
def run_athena_query(query, database, s3_output_location):
    athena_client = boto3.client('athena', region_name=os.environ['region'])
    s3_client = boto3.client('s3', region_name=os.environ['region'])
    queryrunning = 0

    # Kickoff the Athena query
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output_location
        }
    )

    # Log the query execution id
    logger.info('Execution ID: ' + response['QueryExecutionId'])

    # wait for query to finish.
    while queryrunning == 0:
        time.sleep(2)
        status = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        results_file = status["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        if status["QueryExecution"]["Status"]["State"] != "RUNNING":
            queryrunning = 1

    # parse the s3 URL and find the bucket name and key name
    s3url = urlparse(results_file)
    s3_bucket = s3url.netloc
    s3_key = s3url.path

    # download the result from s3
    s3_client.download_file(s3_bucket, s3_key[1:], "/tmp/results.csv")

    # Parse file and update the data to DynamoDB
    # This example will only have one record per petric so always grabbing 0
    metric_value = 0
    with open("/tmp/results.csv", newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            metric_value = row['_col0']

    os.remove("/tmp/results.csv")
    return metric_value


# Save result to DDB for fast access from Alexa/Lambda
def upsert_into_DDB(nm, value, context):
    region = os.environ['region']
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(os.environ['vpa_ddb_table'])
    try:
        response = table.put_item(
            Item={
                'metric': nm,
                'value': value
            }
        )
        return 0
    except Exception:
        logger.error("ERROR: Failed to write metric to DDB")
        return 1