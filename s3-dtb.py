import boto3 as bt
import botocore

BUCKET_NAME = 'nis3-mh100-072418'
KEY         = 0

s3 = bt.resource('s3')
s3_client = bt.client('s3')

# display bucket list
for bucket in s3.buckets.all():
    print ( bucket.name )

# create config for a specific table
cors_configuration = {
    'CORSRules': [{'AllowedHeaders':['Authroization'],
                   'AllowedMethods': ['GET', 'PUT'],
                   'AllowedOrigins': ['*'],
                   'ExposeHeaders': ['GET', 'PUT'],
                   'MaxAgeSeconds': 3000}]
}

s3_client.put_bucket_cors( Bucket= 'mercs-mh100-3107',
                             CORSConfiguration = cors_configuration)

# result = s3.get_bucket_cors( Bucket= 'mercs-mh100-3107',
#                              CORSConfiguration = cors_configuration)
# print (result)

# download file
try:
    s3.Bucket(BUCKET_NAME).download_file('Cycler_Data_Merc_180731.csv', r'/media/kacao/479E26136B58509D/Titan_AES/Dynamo/data/xxx.csv')
# s3_client.download_file( 'nis3-mh100-072418', 'Cycler_Data_Merc_180731.csv')

except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == '404':
        print ('The object does not exist')
    else:
        raise