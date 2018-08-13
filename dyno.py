from boto3 import resource
from boto3.dynamodb.conditions import Key

# The boto3 dynamoDB resource
dynamodb = resource('dynamodb')
table = dynamodb.Table('nis3')

# read the setting of table
def get_table_metadata( tablename ):
    """
    Get some setting about the chosen table
    :param tablename:
    :return:
    """
    table = dynamodb.Table( tablename )

    return {
        'num_items': table.item_count,
        'primary_key_name' : table.key_schema[0],
        'status' : table.table_status
    }

def read_table_item( table_name, pk_name, pk_value ):
    """
    :param table_name:
    :param pk_name: column/header name to scan
    :param pk_value:
    :return: item read by primary key
    """
    table = dynamodb.Table( table_name )
    response = table.get_item( Key = {pk_name: pk_value})
    return response

def add_item( table_name, col_dict ):
    """
    :param table_name:
    :param pk_name:
    :param pk_value:
    :return: add one item (row) to table. col_dict is a dictionary
    {col_name:value}
    """
    table = dynamodb.Table( table_name, col_dict )
    response = table.put_item( Item = col_dict )
    return response

def delete_item( table_name, pk_name, pk_value ):
    """
    Delete an item (row) from its primary key
    :param table_name:
    :param pk_name:
    :param pk_value:
    :return:
    """
    return response


response = read_table_item( 'nis3', 'name', 'leaf')
print ( response )
print ( response['Item'] )
# numitem, key, status = get_table_metadata( 'mytesttable')
# print ( 'item values: {}, {}, {}'.format(numitem, key, status))

