import json
import boto3
import os
import re
import base64
import time

s3client = boto3.client('s3')
dynamoclient = boto3.client('dynamodb')
rekognitionclient = boto3.client('rekognition')
snsclient = boto3.client('sns')
source_bucket = os.environ['source_bucket']
images_bucket = os.environ['images_bucket']
collection = os.environ['collection']

def grab_image(text, timestamp):
    # grabbing the base64 encoded version
    stripped = text.replace('\n','').replace('\r','')
    image_list = (re.search('Content-Disposition: inline(\/9j\/.*)--_', stripped)).group(1)
    #print image_list

    # decoding base64 string into byte array as the rekognition SDK does the encoding
    # to base64
    new_image = bytearray(base64.b64decode(image_list))

    # uploading image to s3 bucket
    s3client.put_object(
        Body=new_image,
        Bucket=images_bucket,
        Key=timestamp+".png"
    )
    return new_image

def process_labels(label, location, records_labels_list, label_list, exclusion_list):
    # We want to store the entire record for historical purposes and 
    # to use for comparison.  We need to convert it into DynamoDB JSON
    holder={
        'M':{
            'Name':{
                "S":label['Name']
            },
            'Confidence':{
                "S":str(label['Confidence'])
            }
        }
    }
    # adding to our list that we will put_item() later
    records_labels_list.append(holder)

    # we also want to do a comparison of just labels
    # later to see what's changed since our last motion
    # appending our name to our label list
    if label['Name'] not in exclusion_list:
        label_list.append(label['Name'])

        # updating our counters for each individual label in dynamo
        dynamoclient.update_item(
            TableName=os.environ['dynamo_labels_count'],
            Key={
                'location': {"S":location},
                'label': {"S":label['Name']}
            },
            UpdateExpression='ADD #count :val',
            ExpressionAttributeNames={
                '#count' : "count"
            },
            ExpressionAttributeValues={
                ':val': {"N":"1"}
            }
        )
    return records_labels_list, label_list

def get_labels(new_image, location):
    # using rekognition to detect labels in the photo
    response = rekognitionclient.detect_labels(
        Image={
            'Bytes': new_image
        },
        MinConfidence=60
    )
    #print(json.dumps(response))
    raw_labels = response['Labels']
    # our placeholders
    records_labels_list = []
    label_list = []
    exclusion_list = []
    # need to get our list of things to ignore
    exclusion_response = dynamoclient.get_item(
        TableName=os.environ['dynamo_exclusion'],
        Key={
            'location':{"S":location}
        }
    )
    if "Item" not in exclusion_response:
        print "No exclusion list setup for %s" % location
    else:
        exclusion_list = exclusion_response['Item']['labels']['SS']

    # iterating through each of the labels we get back from rekognition
    for label in raw_labels:
        records_labels_list, label_list = process_labels(label, location, records_labels_list, label_list, exclusion_list)

    return records_labels_list, label_list

def determine_diff(location, label_list, message):
    # so getting last record we stored
    query_response = dynamoclient.query(
        TableName=os.environ['dynamo_records'],
        Limit=1,
        KeyConditionExpression='#location = :location',
        ExpressionAttributeValues={
            ':location':{"S":location}
        },
        ExpressionAttributeNames={
            '#location':"location"
        },
        ScanIndexForward=False
    )
    #print query_response
    last_seen_label_list=[]
    if query_response['Items']:
        for label_response in query_response['Items'][0]['sanitized_labels']['SS']:
            last_seen_label_list.append(label_response)


    #label_list.append('dog')

    # compare whats different from what we just saw and what happened in the last clip
    if list(set(label_list).difference(last_seen_label_list)):
        message.append("New items compared to last time: %s" % ", ".join(list(set(label_list).difference(last_seen_label_list))))
    else:
        message.append("Nothing new compared to last time")
    if list(set(last_seen_label_list).difference(label_list)):
        message.append("Items not seen compared to last time: %s" % ", ".join(list(set(last_seen_label_list).difference(label_list))))
    else:
        message.append("Nothing missing compared to last time")

    return message

def facial_recognition(message, timestamp):
    response = rekognitionclient.index_faces(
        CollectionId=collection,
        Image={
            'S3Object': {
                'Bucket': images_bucket,
                'Name': timestamp+".png"
            }
        }
    )
    if not response['FaceRecords']:
        message.append("No Faces Detected")
        return message
    
    face_matches=[]
    unrecognized_faces=0
    unknown_name=0
    # iterating through the faces found in the submitted image
    for face in response['FaceRecords']:
        face_id = face['Face']['FaceId']
        print face_id
        response = rekognitionclient.search_faces(
            CollectionId=collection,
            FaceId=face_id
        )
        # checking if there were any matches
        if not response['FaceMatches']:
            # never seen this face before
            unrecognized_faces+=1
            continue

        # we recognized a previously seen face
        for match in response['FaceMatches']:
            if "ExternalImageId" in match['Face'] and match['Face']["ExternalImageId"] != '':
                face_matches.append(match['Face']['ExternalImageId'])
            else:
                # we've seen this face before, but don't know a name
                unknown_name+=1

    message.append("Faces Detected!")
    if face_matches:
        message.append("Detected: %s" % ', '.join(face_matches))
    if unknown_name:
        message.append("Recognized but no name: %s" % unknown_name)
    if unrecognized_faces:
        message.append("Never seen before: %s" % unrecognized_faces)

    return message

def lambda_handler(event, context):
    #print json.dumps(event)
    # basic setup things
    timestamp = str(int(time.time()))
    print timestamp
    message=[]
    # iterating through each of the records received
    for email_record in event['Records']:
        s3_key = email_record['ses']['mail']['messageId']
        print "S3 Key: %s" % s3_key
        # download email message from s3 so we can extract the base64 image
        s3client.download_file(Bucket=source_bucket, Key=s3_key, Filename="/tmp/" + s3_key)
        with open("/tmp/" + s3_key, 'r') as in_file:
            text = in_file.read()

        # grabbing our location from the email
        location = (re.search('Subject: Motion Detected: (.*)', text)).group(1)
        # removing \r at the end
        location = location.strip()
        message.append("Location: %s" % location)

        # extract our image out of the message email (base64 encoded)
        new_image = grab_image(text, timestamp)

        # take our image and extract our labels
        records_labels_list, label_list = get_labels(new_image, location)
        message.append("Items seen: %s " % ", ".join(label_list))

        # need to see what's changed since last time
        message = determine_diff(location, label_list, message)
        
        #storing all labels we just saw in dynamo for records
        dynamoclient.put_item(
            TableName=os.environ['dynamo_records'],
            Item={
                'location': {"S":location},
                'timestamp': {"N":timestamp},
                'labels':{"L":records_labels_list},
                'sanitized_labels':{"SS":label_list}
            }
        )
        # call index faces to get a list of all faces in our image
        message = facial_recognition(message, timestamp)
        # region is hardcoded...needs to be env var
        message.append("https://s3-us-west-2.amazonaws.com/%s/%s.png" % (images_bucket , timestamp))
        ####
    print "\n\n".join(message)
    # publish message to SNS
    snsclient.publish(
        TopicArn="arn:aws:sns:us-west-2:227518703200:unifi-motion",
        Message="\n\n".join(message)
    )


if __name__ == '__main__':
    event={
    "Records": [
        {
            "eventVersion": "1.0",
            "ses": {
                "mail": {
                    "messageId": "mhs71vv12rsuglfsh23arre413koi883sudggdg1"
                }
            }
        }
    ]
}
    lambda_handler(event, None)