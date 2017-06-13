import boto3

s3client=boto3.client('s3')
rekognitionclient=boto3.client('rekognition')

bucket='unifi-faces-training'

response = s3client.list_objects(
    Bucket=bucket
)

for file in response['Contents']:
    key = file['Key']
    print key
    name = key[0:key.find('-')]

    response = rekognitionclient.index_faces(
        CollectionId='faces',
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        ExternalImageId=name
    )
    print response
