"""Read the isolated router's full backfill log through Systems Manager."""
import boto3
import time

client = boto3.client('ssm', region_name='us-east-1')
instance = 'i-0eaee37f64dfe7195'
command = client.send_command(InstanceIds=[instance], DocumentName='AWS-RunShellScript',
    Parameters={'commands': ['tail -n 35 /home/ec2-user/DCIO-pipeline/DCIO-pipeline/stable_value_router_runtime_20261010_01/full_apply.log'], 'executionTimeout': ['60']})['Command']['CommandId']
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    try:
        result = client.get_command_invocation(CommandId=command, InstanceId=instance)
    except client.exceptions.InvocationDoesNotExist:
        time.sleep(1)
        continue
    if result['Status'] not in {'Pending', 'InProgress', 'Delayed'}:
        print(result['Status'])
        print(result['StandardOutputContent'])
        print(result['StandardErrorContent'])
        break
    time.sleep(1)
else:
    print('Status request still pending:', command)
