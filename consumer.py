
# from kafka import KafkaConsumer
# from json import loads

# consumer = KafkaConsumer(
#    'test',
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-group-1',
#     api_version=(2,8,1),
#     value_deserializer=lambda m: loads(m.decode('utf-8')),
#     bootstrap_servers=['172.17.0.1:32783','172.17.0.1:32782','172.17.0.1:32781'])

# for m in consumer:
#     print(m.value)


from kafka import KafkaConsumer
consumer = KafkaConsumer('example_topic')
for msg in consumer:
    print (msg.value)