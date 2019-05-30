import boto3
import time

ACCESS_KEY = 'ACCESS_KEY' # add access key
SECRET_KEY = 'SECRET_KEY' # add secret key
REGION = 'eu-west-1'

client = boto3.client(
    'dynamodb',
    region_name=REGION,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# Display counter on console
counter = 0

def dynamo_updater():
    global counter

    dynamodb = boto3.resource(
        'dynamodb',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    table = dynamodb.Table('Articles') # change the table name

    response = table.scan(
        Select='SPECIFIC_ATTRIBUTES',
        AttributesToGet=[
            'id' # list of fields to select
        ]
    )

    for i in response['Items']:
        counter +=1
        update_dynamdo_db(table, i)

    while 'LastEvaluatedKey' in response:
        time.sleep(2)
        response = table.scan(
            Select='SPECIFIC_ATTRIBUTES',
            AttributesToGet=[
                'id',
            ],
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        for i in response['Items']:
            counter +=1
            update_dynamdo_db(table, i)


def update_dynamdo_db(table, i):
    print("Updation for article id ", i['id'], " starts and counter is: ", counter)
    time.sleep(2)
    # sleep system for 2 sec and give time to CMS
    response = table.update_item(
        Key={'id': str(i['id'])},
        UpdateExpression="SET #att = :att, #cy = :cy",
        ExpressionAttributeValues={
            ':att': str(i['title']).strip(),
            ':cy': str(i['year']).strip(),
        },
        ExpressionAttributeNames = {
            '#att': 'title',
            '#cy': 'article-year' # if you have field with '-'
        }
    )
    # print("Updation for ", counter, " ends")


if __name__ == '__main__':
    dynamo_updater()
