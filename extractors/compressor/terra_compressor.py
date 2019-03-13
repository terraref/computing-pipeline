import os
import subprocess
import pika


RMQ_URI     = os.getenv("RABBITMQ_URI", "amqp://guest:guest@127.0.0.1/%2f")
RMQ_XCHNG   = os.getenv("RABBITMQ_EXCHANGE", "clowder")
RMQ_QUEUE   = os.getenv("RABBITMQ_QUEUE", "terra.compressor")

def connect():
    params = pika.URLParameters(RMQ_URI+"?heartbeat_interval=300")
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(queue=RMQ_QUEUE, durable=True)
    channel.exchange_declare(exchange=RMQ_XCHNG, exchange_type='topic', durable=True)
    channel.queue_bind(queue=RMQ_QUEUE, exchange=RMQ_XCHNG, routing_key=RMQ_QUEUE)
    return channel

def fetch_job(chan, method, properties, body):
    # Get next message containing a dirpath from RabbitMQ
    files = os.listdir(body)
    for f in files:
        if f.endswith(".tif"):
            f_path = os.path.join(body, f)
            compress(f_path)
    chan.basic_ack(delivery_tag=method.delivery_tag)

def compress(input_file):
    print("Compressing %s" % input_file)
    temp_out = input_file.replace(".tif", "_compress.tif")
    subprocess.call(["gdal_translate", "-co", "COMPRESS=LZW", input_file, temp_out])
    if os.path.isfile(temp_out):
        os.remove(input_file)
        os.rename(temp_out, input_file)


channel = connect()
channel.basic_consume(fetch_job, queue=RMQ_QUEUE, no_ack=False)
channel.start_consuming()
