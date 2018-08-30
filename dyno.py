"""
install aws-cli and aws cmdline tool
set up account and create signature keypass

install boto3

"""

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
            # {
            #     'AttributeName': 'id',
            #     'KeyType': 'RANGE'
            # }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'index',
                'AttributeType': 'N'
            },
            # {
            #     'AttributeName': 'id',
            #     'AttributeType': 'S'
            # }
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
        'status' : table.table_status,
        'bytes_size': table.table_size_bytes,
        'global_secondary_indices': table.global_secondary_indexes
    }


def read_table_item( table_name, pk_name, pk_value ):
    """
    if try to getItem with only specifying a Hash or only a Range key, 
    we we face Validation exception
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


def batch_request( table_name, low_bound, high_bound, item ):
    table = dynamodb.Table( table_name )
    with table.batch_writer() as batch:
        for i in range(low_bound, high_bound):
            batch.put_item(
                Item = item
            )
    return


def scan_table( table_name, filter_key=None, filter_value=None):
    """
    Scan through the table to find a matching key + value
    """
    table = dynamodb.Table( table_name )
    if filter_key and filter_value:
        filtering_exp = Key( filter_key ).eq( filter_value )
        response = table.scan(FilterExpression = filtering_exp )
    else:
        response = table.scan()

    return response


def query_table( table_name,filter_key=None, filter_value=None ):

    return



def _is_matching_name( string, text ):
    if text in string:
        return True
    return False


def get_table_list( ):
    cmd = 'aws dynamodb list-tables'
    cmd_string = (Popen(cmd))
    print(cmd_string)
    return json.loads(cmd_string.decode('utf-8'))


#==============================================================================#
#=============================== MAIN =========================================#
def main():

    tbName = 'Nissan'
    table_list = get_table_list()
    print(table_list["TableNames"][0])

    # find matching table name
    _match_counter = 0
    for i in range(len(table_list["TableNames"])):
        if _is_matching_name(tbName, table_list['TableNames'][i]):
            _match_counter += 1
            break

    if _match_counter == 0:
        create_new_table( tbName )
        print('new table created successully')
    else:
        print('using an existing table on cloud')

    # a dummy data to add
    item = {
        'index':3,
        'id':'three',
        'id_num':'CC_Chg',
        'time':'13-08-23',
        'current':254,
        'cap(mAh)':247,
        'cap(microAh)':15963,
        'Date/Time':'2018-08-17',
    }
    response = add_item( tbName, item )
    print ( response )


    # key = { 'index':1,
    #         'id':'two'}
    # key_1 = {'index', 'id'}
    # key_2 = {1, 'two'}
    response = read_table_item( tbName, 'index', 2)
    print(response['Item'])
    # response = delete_item( tbName, 'index', 1 )
    # print ('\n')

    item = {
        'id': 'cycle_' + str(i),
        'id_num': 'CC_Chg' if i % 2 == 1 else 'CCCV_Chg',
        'time': '13-08-23',
        'current': 254 + i*3,
        'cap(mAh)': 247 + i*5,
        'cap(microAh)': 15963 + i*500,
        'Date/Time': '2018-08-17',
    }

    # table = dynamodb.Table(tbName)
    # with table.batch_writer() as batch:
    #     for i in range(3, 6):
    #         batch.put_item(
    #             Item={
    #
    #             }
    #         )


    #for a table using Hasg and Range key type
    # table = dynamodb.Table(tbName)
    # response = table.get_item(
    #     Key={
    #         'index':3,
    #         'id':'three',
    #     }
    # )
    # response = table.delete_item(
    #     Key={
    #         'index': 6,
    #         'id':'six'
    #     }
    # )
    # print (response)


    metaData = get_table_metadata( tbName )
    print (metaData)
    print ( 'item values: {}, {}, {}'.format(metaData['num_items'],
                                             metaData['primary_key_name'],
                                             metaData['status'],
                                             metaData['bytes_size'],
                                             metaData['global_secondary_indices']))



if __name__ == '__main__':
    main()

