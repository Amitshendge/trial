# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------


from typing import final
import pandas as pd
import json
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.eventhub.exceptions import EventHubError 
import random
import datetime
import pytz
from azure.storage.queue import QueueClient,BinaryBase64EncodePolicy,BinaryBase64DecodePolicy

connection_str='Endpoint=sb://smartfactorydemo2.servicebus.windows.net/;SharedAccessKeyName=Rootaccesspolicy;SharedAccessKey=zCjPN92aD/vFuDu/SxhYp1A29Dfv6ceis7T3VRNTxAw=;EntityPath=weightsensorfeed'
eventhub_name='weightsensorfeed'
connq='DefaultEndpointsProtocol=https;AccountName=cohortdataengg;AccountKey=Ib4rKMs9yjmfhhu1JxshP3oTQr30pSeuQY+9Kc5pzzQ3xlpOgB0xfh3QkTtDu3iXg/iYBag0HIhRPRfd7E9qnQ==;EndpointSuffix=core.windows.net'

def weightsensor():
    
    topicnamelist=["Device 1","Device 2"]
    topicnamevalues=random.choice(topicnamelist)
    measurementvalues=round(random.uniform(3.60, 3.90), 2)
    unitvalues="oz"
    plantvalueslist=["New Jersey"]
    plantvalues=random.choice(plantvalueslist)
    linevalues="Granola"
    productlist= ["Peanut Butter Granola"]
    productvalues= random.choice(productlist)
    environmentlist=["PROD"]
    environmentvalues=random.choice(environmentlist)
    deviceNamevalue="raspberrypi"
    recordTSvalue=str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')))
   
    weight_sensor_dict={
     "topicName":topicnamevalues,
     "measurement":measurementvalues,
     "unit":unitvalues,
     "plant":plantvalues,
     "line": linevalues,
     "product": productvalues,
     "environment": environmentvalues,
     "deviceNamevalue":deviceNamevalue,
     "recordTS": recordTSvalue

     }

    return weight_sensor_dict

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    while True:
        await asyncio.sleep(5)
        producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)
        async with producer:
        # Create a batch.
            event_data_batch = await producer.create_batch()

            # Add events to the batch.
            event_data_batch.add(EventData(json.dumps(weightsensor())))
            #event_data_batch.add(EventData(json.dumps(dict1)))
           

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
            print('Success sent to Azure Event Hubs')
            que_message()
def que_message():
    base64_queue_client = QueueClient.from_connection_string(
                            conn_str=connq, queue_name='trial',
                            message_encode_policy = BinaryBase64EncodePolicy(),
                            message_decode_policy = BinaryBase64DecodePolicy()
                        )
    input_message=str("message").encode('utf8')
    base64_queue_client.send_message(input_message)
    print("message added to queue")


loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(run())
    loop.run_forever()
except  KeyboardInterrupt:
    pass
finally:
    print("ClosingLoopNow")
    loop.close()

