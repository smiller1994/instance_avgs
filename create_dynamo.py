import boto3
def main():
    dynamo=boto3.resource("dynamodb", region_name="us-east-2")
    table_out= dynamo.create_table(
    TableName= "instance_avgs",
    KeySchema=[
        {
            'AttributeName':'Instance_Type',
            'KeyType':'HASH'
        },
        {
            'AttributeName':'Average_Time',
            'KeyType':'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName':'Instance_Type',
            'AttributeType': 'S'},
        {
            'AttributeName':'Average_Time',
            'AttributeType':'S'
        },],
     ProvisionedThroughput={
         'ReadCapacityUnits':20,
         'WriteCapacityUnits':20,
     }       
   )
if __name__ == '__main__':
	main()