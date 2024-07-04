import json
import boto3

def write_manually(bucketname, key, response):
    # Mensaje JSON que deseas enviar
    message = {
        "Video": {
            "S3Bucket": bucketname,
            "S3ObjectName": key
        },
        "JobId": response["JobId"],
        "Status": "SUCCEEDED"
    }
    
    # Convertir el mensaje a formato JSON
    message_json = json.dumps(message)
    
    # Crear cliente SNS
    sns_client = boto3.client('sns')
    
    # Nombre del tema SNS al que enviar el mensaje
    topic_arn = 'arn:aws:sns:us-east-2:211125485640:AmazonRekognitionVideoStatus'  # SNS
    
    # Publicar el mensaje al tema SNS
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_json
    )
    
    # Imprimir la respuesta del servicio SNS
    print("response de sns: ",response)
    
    # Retornar un mensaje de éxito (opcional)
    return {
        'statusCode': 200,
        'body': json.dumps('Mensaje enviado correctamente al tema SNS.')
    }

def start_label_detection(bucketname, key):
    client = boto3.client('rekognition')
    response = client.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': bucketname,
                'Name': key
            }
        },
        MinConfidence=40,
        NotificationChannel={
            'SNSTopicArn': 'arn:aws:sns:us-east-2:211125485640:AmazonRekognitionVideoStatus',  # SNS Topic
            #'RoleArn': 'arn:aws:iam::211125485640:role/service-role/startLabelDetection-role-g6cvearj' # startLabelDetection-role-g6cvearj
            'RoleArn': 'arn:aws:iam::211125485640:role/AmazonRekognitionVideoAccess'  # AmazonRekognitionVideoAccess 
        }
    )
    print("label response: ", response)
    #Porque no funcionaba integrar automaticamente
    write_manually(bucketname, key, response)

    

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
    print("bucketname: ",bucketname)
    print("key: ",key)
    start_label_detection(bucketname, key)
    #invoke_mediaconvert(bucketname, key)
