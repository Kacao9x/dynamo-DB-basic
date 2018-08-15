from boto3 import resource
from boto3.dynamodb.conditions import Key
import subprocess
import json


#==============================================================================#

#Subprocess's call command with piped output and active shell
def Call(cmd):
    return subprocess.call(cmd, stdout=subprocess.PIPE,
                           shell=True)

#Subprocess's Popen command with piped output and active shell
def Popen(cmd):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            shell=True).communicate()[0].rstrip()

#Subprocess's Popen command for use in an iterator
def PopenIter(cmd):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            shell=True).stdout.readline

#==============================================================================#


# The boto3 dynamoDB resource
dynamodb = resource('dynamodb')


def create_new_table ( tablename ):
    table = dynamodb.create_table (
        TableName = tablename,
        KeySchema=[
            {
                'AttributeName' : 'index',
                'KeyType':'HASH'
            },
            {
                'AttributeName': 'id',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'index',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName = tablename)
    print (table.item_count)
    return


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
    response = table.get_item( Key = {pk_name : pk_value})
    return response


def add_item( table_name, col_dict ):
    """
    :param table_name:
    :param pk_name:
    :param pk_value:
    :return: add one item (row) to table. col_dict is a dictionary
    {col_name:value}
    """
    table = dynamodb.Table( table_name )
    response = table.put_item( Item = col_dict )
    return response


def delete_item( table_name, pk_name, pk_value ):
    """
    Delete an item (row) from its primary key
    """
    table = dynamodb.Table( table_name )
    table.delete_item(Key={pk_name : pk_value})
    return


def _is_matching_string( string, text ):
    if text in string:
        return True
    return False


cmd = 'aws dynamodb list-tables'
cmd_string = (Popen(cmd))
print (cmd_string)
table_list = json.loads( cmd_string )
print ( table_list["TableNames"][0])

tbName = 'Me02-H100_180811'
_match_counter = 0

for i in range( len(table_list["TableNames"]) ):
    if _is_matching_string( tbName, table_list['TableNames'][ i ]):
        _match_counter += 1
        break

if _match_counter == 0:
    create_new_table( tbName )
    print( 'new table created successully')
else:
    print( 'using an existing table on cloud')





tbName = 'Me02-H100_180811'
# create_new_table( tbName )

item = {
    'index':4,
    'id':'five',
    'id_num':'CV_Chg',
    'time':'01',
    'current':150,
    'cap(mAh)':162,
    'cap(microAh)':117552,
    'Date/Time':'2018-08-14',
}


response = add_item( tbName, item )
print ( response )

#
# key = { 'index':1,
#         'id':'two'}
# key_1 = {'index', 'id'}
# key_2 = {1, 'two'}
# response = read_table_item( tableName, key_1, key_2)
# response = table.get_item( Key = {pk_name : pk_value})

table = dynamodb.Table(tbName)
response = table.get_item(
    Key={
        'index':3,
        # 'id':'three',
        # 'id_num':'CC_Chg',
    }
)

# print ( response['Item'] )


# delete_item( tableName, 'id', 'two' )


# response = read_table_item( tbName, ['index','id'], [3, 'three'])
print ( response['Item'] )
# print ( 'item values: {}, {}, {}'.format(numitem, key, status))



