#Start label detection
import json
import boto3
from botocore.errorfactory import ClientError
import os

def start_label_detection(bucketname, key):
    client = boto3.client('rekognition')
    response = client.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': bucketname,
                'Name': key
            }
        },
        MinConfidence=20,
        """NotificationChannel={
            'SNSTopicArn': 'arn:aws:sns:us-east-2:211125485640:AmazonRekognitionVideoStatus',  # SNS Topic
            'RoleArn': 'arn:aws:iam::211125485640:role/service-role/startLabelDetection-role-g6cvearj' # startLabelDetection-role-g6cvearj
            #'arn:aws:iam::211125485640:role/AmazonRekognitionVideoAccess'  # AmazonRekognitionVideoAccess 
        }"""
    )
    print("label response: ", response)
    

def invoke_mediaconvert(bucket, key):
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName='YourFunctionName',  # Función de MediaConvert
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps({"bucket": bucket, "key": key})
    )

def lambda_handler(event, context):
    bucketname = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    start_label_detection(bucketname, key)
    #invoke_mediaconvert(bucketname, key)


#process Label detection


'''
Invokes Lambda function #4 that converts JPEG images to GIF
Triggers SNS in the event of Label Detection Job Failure.
Writes Labels (extracted through Rekognition) as JSON in S3 bucket.
Creates JSON tracking file in S3 that contains a list pointing to: Input Video path, Metadata JSON path, Labels JSON path, and GIF file Path.
'''

#Invoke Lambda function: giftranscode
def invoke_gif(bucket, key, jobID):
    #invokes another lambda which uses elastic transcoder to create a GIF file
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName='giftranscode',
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps({"bucket": bucket, "key": key, "jobID": jobID})
        )
    
def shrinkLabels(labels):#Capaz para hacer un wordcount
    outlabels = []
    for item in labels:
        ismatch = False
        instances = item.get("Label", {}).get("Instances", [])
        if instances != []:
            ismatch = True
        if ismatch:
           outlabels.append(item)
    return  outlabels
    
def WriteObjectToS3AsJson(thisObject, bucket, key):
    client = boto3.client('s3')
    jsonobject = json.dumps(thisObject)
    response = client.put_object(Body=bytes(jsonobject, 'utf-8'),Bucket=bucket, Key = key)

def ReadFileAsJsonFromS3(bucket, key):
    client = boto3.client('s3')
    try:
        response = client.get_object(Bucket=bucket,Key=key)
    except ClientError:
        return []
    jsonasRawText = response['Body'].read().decode('utf-8')
    loadedDoc = json.loads(jsonasRawText)
    return loadedDoc

def AddUpdateProjectTracking(newObject):
    #we update video entry meta data in the index JSON file if they do not exist
    #if the video is not yet in the index JSON, it gets added
    bucket = os.environ["labelsbucketname"]
    key = os.environ["labelsoutput"]
    currentList = ReadFileAsJsonFromS3(bucket, key)
    videosList = []
    newList = []
    for item in reversed(currentList):
        if item["rawvideopath"] != newObject["rawvideopath"]:
            if item["rawvideopath"] not in videosList:
                newList.append(item)
                videosList.append(item["rawvideopath"])
    newList.append(newObject)
    WriteObjectToS3AsJson(newList, bucket, key)

    #AddUpdateProjectTracking(newTrackObject) # No se porque esta aqui

def SNSfailure(Message):
    print("Label detection job failed:", Message)
    # Handle failure (e.g., notify via SNS, log, etc.)

def get_label_detection(jobid, bucket, objectname, SortBy='TIMESTAMP'):
    client = boto3.client('rekognition')
    response = client.get_label_detection(
        JobId=jobid,
        SortBy=SortBy
    )
    labels = response['Labels']
    # Process and return the labels as needed
    return labels

def lambda_handler(event, context):
    print(json.dumps(event))
    for record in event['Records']:
        Message=json.loads(record['Sns']['Message'])
        print(Message)
        s3bucket=Message['Video']['S3Bucket']
        jobid=Message['JobId']
        status=Message['Status']
        if status != 'SUCCEEDED':
            SNSfailure(Message)
        else:
            s3objectname = Message['Video']['S3ObjectName']
            labels = get_label_detection(jobid, s3bucket, s3objectname, SortBy='TIMESTAMP')
            
            # Write labels to S3 as JSON
            labels_key = f"labels/{jobid}.json"
            WriteObjectToS3AsJson(labels, s3bucket, labels_key)
            
            # Invoke GIF creation Lambda
            #invoke_gif(s3bucket, s3objectname, jobid)
            
            # Define paths
            metadata_json_path = f"metadata/{jobid}.json"  # Assume metadata is stored here
            gif_path = f"gifs/{jobid}.gif"  # Assume GIF is stored here
            
            # Create tracking object
            newTrackObject = {
                "rawvideopath": s3objectname,
                "metadatajsonpath": metadata_json_path,
                "labelsjsonpath": labels_key,
                "giffilepath": gif_path
            }
            AddUpdateProjectTracking(newTrackObject)
            
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }