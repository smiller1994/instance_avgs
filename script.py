import boto3
from datetime import datetime, timedelta

def main():
    table_name = "instance_avgs"
    global num 
    num = 0
    instances=['vertica','spark','ldp','talend','linux_python','pyspark']
    
    vertica = []
    spark = []
    ldp = []
    talend = []
    linux_python = []
    pyspark = []
    
    dynamo=boto3.resource("dynamodb", region_name="us-east-2")
    table = dynamo.Table("dynamite-dev")
    response = table.scan()
    data = response['Items']
    
    
    for adict in data:
        if adict.get('type') == instances[0]:
            name_vertica = 'vertica'
            vertica_count=0.0
            times = {k: v for k, v in adict.items() if k == "start_dttm" or k == "delete_dttm"}
            x = [datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f") for date in times.values()]
            vertica.append((x[0]-x[1]))
        elif adict.get('type') == instances[1]:
            name_spark = 'spark'
            spark_count=0.0
            times = {k: v for k, v in adict.items() if k == "start_dttm" or k == "delete_dttm"}
            x = [datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f") for date in times.values()]
            spark.append((x[0]-x[1]))
        elif adict.get('type') == instances[2]:
            name_ldp = 'ldp'
            ldp_count=0.0
            times = {k: v for k, v in adict.items() if k == "start_dttm" or k == "delete_dttm"}
            x = [datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f") for date in times.values()]
            ldp.append((x[0]-x[1]))
        elif adict.get('type') == instances[3]:
            name_talend = 'talend'
            talend_count=0.0
            times = {k: v for k, v in adict.items() if k == "start_dttm" or k == "delete_dttm"}
            x = [datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f") for date in times.values()]
            talend.append((x[0]-x[1]))
        elif adict.get('type') == instances[4]:
            name_linux_python = 'linux_python'
            linux_python_count=0.0
            times = {k: v for k, v in adict.items() if k == "start_dttm" or k == "delete_dttm"}
            x = [datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f") for date in times.values()]
            linux_python.append((x[0]-x[1]))
        elif adict.get('type') == instances[5]:
            name_pyspark = 'pyspark'
            pyspark_count=0.0
            times = {k: v for k, v in adict.items() if k == "start_dttm" or k == "delete_dttm"}
            x = [datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f") for date in times.values()]
            pyspark.append((x[0]-x[1]))
        else:
             print("Can't determine type.")
                
    table_out=dynamo.Table(table_name)
    
    for t in vertica:
        seconds=float(t.total_seconds())
        mins=round(seconds/60,3)
        vertica_count+=mins
        vertica_avg =round(vertica_count/len(vertica),2)
        vertica_avg = str(vertica_avg)
    print('Avg Time for a {} instance: {} minutes.'.format(name_vertica,round(vertica_count/len(vertica),2)))
    
    table_out.put_item(
        Item={
            'Instance_Type':name_vertica,
            'Average_Time': vertica_avg,
        })
    for t in spark:
        seconds=float(t.total_seconds())
        mins=round(seconds/60,3)
        spark_count+=mins
        spark_avg=round(spark_count/len(spark),2)
        spark_avg=str(spark_avg)
    table_out.put_item(
        Item={
            'Instance_Type':'Spark',
            'Average_Time': spark_avg,
        })
    print('Avg Time for a {} instance: {} minutes.'.format(name_spark, round(spark_count/len(spark),2)))
    
    for t in ldp:
        seconds=float(t.total_seconds())
        mins=round(seconds/60,3)
        ldp_count+=mins
        ldp_avg=round(ldp_count/len(ldp),2)
        ldp_avg=str(ldp_avg)
    print('Avg Time for a {} instance: {} minutes.'.format(name_ldp, round(ldp_count/len(ldp),2)))   
    
    table_out.put_item(
        Item={
            'Instance_Type':'LDP',
            'Average_Time': ldp_avg,
        })
    for t in talend:
        seconds=float(t.total_seconds())
        mins=round(seconds/60,3)
        talend_count+=mins
        talend_avg=round(talend_count/len(talend),2)
        talend_avg=str(talend_avg)
    table_out.put_item(
        Item={
        'Instance_Type':'Talend',
        'Average_Time':talend_avg,
        })
    print('Avg Time for a {} instance: {} minutes.'.format(name_talend, round(talend_count/len(talend),2)))
    
    for t in linux_python:
        seconds=float(t.total_seconds())
        mins=round(seconds/60,3)
        linux_python_count+=mins
        linux_python_avg=round(linux_python_count/len(linux_python),2)
        linux_python_avg=str(linux_python_avg)
    table_out.put_item(
        Item={
            'Instance_Type':'Linux Python',
            'Average_Time': linux_python_avg,
        })
    print('Avg Time for a {} instance: {} minutes.'.format(name_linux_python, round(linux_python_count/len(linux_python),2)))
    
    for t in pyspark:
        seconds=float(t.total_seconds())
        mins=round(seconds/60,3)
        pyspark_count+=mins
        pyspark_avg=round(pyspark_count/len(pyspark),2)
        pyspark_avg=str(pyspark_avg)
    table_out.put_item(
        Item={
            'Instance_Type':'Pyspark',
            'Average_Time': pyspark_avg,
        })
    print('Avg Time for a {} instance: {} minutes.'.format(name_pyspark, round(pyspark_count/len(pyspark),2)))
    
if __name__ == "__main__":
    main()
