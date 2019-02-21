import os
import pika

"""
Crawl the listed directories and queue up dataset/plot folders for LZW compressor jobs.
"""

RMQ_URI     = os.getenv("RABBITMQ_URI", "amqp://guest:guest@127.0.0.1/%2f")
RMQ_XCHNG   = os.getenv("RABBITMQ_EXCHANGE", "clowder")
RMQ_QUEUE   = os.getenv("RABBITMQ_QUEUE", "terra.compressor")

tif_roots = ["/home/clowder/sites/ua-mac/Level_1_Plots/rgb_geotiff",
             "/home/clowder/sites/ua-mac/Level_1_Plots/ir_geotiff",
             "/home/clowder/sites/ua-mac/Level_1/rgb_geotiff",
             "/home/clowder/sites/ua-mac/Level_1/ir_geotiff"]


def connect():
    params = pika.URLParameters(RMQ_URI)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(queue=RMQ_QUEUE, durable=True)
    channel.exchange_declare(exchange=RMQ_XCHNG, exchange_type='topic', durable=True)
    channel.queue_bind(queue=RMQ_QUEUE, exchange=RMQ_XCHNG, routing_key=RMQ_QUEUE)
    return channel

channel = connect()
for root_dir in tif_roots:
    dates = os.listdir(root_dir)
    for d in dates:
        date_dir = os.path.join(root_dir, d)
        subdirs = os.listdir(date_dir)
        for sd in subdirs:
            sd_dir = os.path.join(date_dir, sd)
            print("Queuing %s" % sd_dir)
            if not channel.connection.is_open:
                channel = connect()
            channel.basic_publish(RMQ_XCHNG, RMQ_QUEUE, sd_dir)
channel.connection.close()
