import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage

#set connection string and queue name
CONNECTION_STR = os.environ['SERVICE_BUS_CONNECTION_STR']
QUEUE_NAME = os.environ["SERVICE_BUS_QUEUE_NAME"]


def send_single_message(sender):
    #generate message
    message = ServiceBusMessage("Single Message")
    #send message
    sender.send_messages(message)


def send_a_list_of_messages(sender):
    #generate messages
    messages = [ServiceBusMessage("Message in list") for _ in range(10)]
    #send messages
    sender.send_messages(messages)


def send_batch_message(sender):
    #generate a bacth of messages
    batch_message = sender.create_message_batch()
    for _ in range(10):
        try:
            batch_message.add_message(ServiceBusMessage("Message inside a ServiceBusMessageBatch"))
        except ValueError:
            # ServiceBusMessageBatch object reaches max_size.
            # New ServiceBusMessageBatch object can be created here to send more data.
            break
    #sending message batch
    sender.send_messages(batch_message)


servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)
with servicebus_client:
    sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
    with sender:
        send_single_message(sender)
        send_a_list_of_messages(sender)
        send_batch_message(sender)

print("Send message is done.")
