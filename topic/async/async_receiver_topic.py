import os
import asyncio
from azure.servicebus.aio import ServiceBusClient

CONNECTION_STR = os.environ['SERVICE_BUS_CONNECTION_STR']
TOPIC_NAME = os.environ["SERVICE_BUS_TOPIC_NAME"]

servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)

async def main():
    async with servicebus_client:
        # get the Subscription Receiver object for the subscription    
        receiver = servicebus_client.get_subscription_receiver(topic_name=TOPIC_NAME, subscription_name=SUBSCRIPTION_NAME, max_wait_time=5)
        async with receiver:
            for msg in receiver:
                print("Received: " + str(msg))
                # complete the message so that the message is removed from the subscription
                await receiver.complete_message(msg)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
