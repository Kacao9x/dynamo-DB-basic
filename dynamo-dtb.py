import boto3

dynamodb = boto3.resource('dynamodb')

# table = dynamodb.create_table(
#     TableName = 'newTable',
#
#     KeySchema=[
#         {
#             'AttributeName': 'username',
#             'KeyType': 'HASH'
#         },
#         {
#             'AttributeName': 'last_name',
#             'KeyType': 'RANGE'
#         }
#     ],
#
#     AttributeDefinitions=[
#         {
#             'AttributeName': 'username',
#             'AttributeType': 'S'
#         },
#         {
#             'AttributeName': 'last_name',
#             'KeyType': 'S'
#         },
#     ],
#
#     ProvisionedThroughput={
#             'ReadCapacityUnits': 5,
#             'WriteCapacityUnits': 5
#     }
# )
#
# table.meta.client.get_waiter('table_exists').wait(TableName='newTable')
#
# print (table.item_count)
table = dynamodb.Table('nis3')
# print (table.creation_date_time)

table.put_item(
    Item={
        'username': 'titan',
        'first_name': 'kacao',
        'last_name': 'cao',
        'age' : 25
    })