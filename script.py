import boto3
from datetime import datetime, timedelta
global instances
from collections import defaultdict

instances = []

dynamo=boto3.resource("dynamodb", region_name="us-east-2")
table = dynamo.Table("dynamite-dev")
response = table.scan()
data = response['Items']
table_name = "instance_avgs"
table_out=dynamo.Table(table_name)

def add_instance_type(name):
    if name not in instances:
        instances.append(name)

def eval_instance(instance_name):
    a=[]
    inst_count=0
    for adict in data:
        if adict.get('type') == instance_name:
            inst_count+=1
            instance_name_test = instance_name
            times = {k: v for k, v in adict.items() if k == "start_dttm" or k == "delete_dttm"}
            g =[datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f") for date in times.values()]
            g =g[0]-g[1]
            a.append(g)
    converter(a, instance_name_test,inst_count)

def converter(a, instance_name_test,inst_count):
    count = 0
    for t in a:
        seconds=float(t.total_seconds())
        mins=round(seconds/60,3)
        count+=mins
    avg=round(count/len(a),2)
    str_avg= str(avg)
    inst_count = str(inst_count)
    update_dttm = str(datetime.now())
    table_out.put_item(
        Item={
            'Instance_Type': instance_name_test,
            'Instance_Count':inst_count,
            'Average_Time': str_avg,
            'Update_Dttm': update_dttm,
        })
    print('The average time for ' + inst_count + ' counts of a ' + instance_name_test + ' instance: ' + str_avg)

def main():
    for adict in data:
        add_instance_type(adict.get('type'))
    for instance_name in instances:
        eval_instance(instance_name)


if __name__ == "__main__":
    main()
