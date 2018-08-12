from boto3 import resource
from boto3.dynamodb.conditions import Key

# The boto3 dynamoDB resource
dynamodb = resource('dynamodb')
table = dynamodb.Table('nis3')


def get_table_metadata( tablename ):
    table = dynamodb.Table( tablename )

    return {
        'num_items': table.item_count,
        'primary_key_name' : table.key_schema[0],
        'status' : table.table_status
    }

def read_table_item( table_name, pk_name, pk_value ):
    table = dynamodb.Table( table_name )
    response = table.get_item( Key = {pk_name: pk_value})
    return response

read_table_item( 'nis3', 'name', 'leaf')
# numitem, key, status = get_table_metadata( 'mytesttable')
# print ( 'item values: {}, {}, {}'.format(numitem, key, status))

