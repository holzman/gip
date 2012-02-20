
import pika

from gip_common import cp_get, cp_getInt

def connect(cp,):
    username = cp_get(cp, "amqp", "user", "gip")
    # TODO: Haven't decided on the authorization model yet.  For now, we toss
    # around shared secrets in the gip.conf
    password = cp_get(cp, "amqp", "password", "gip")
    hostname = cp_get(cp, "amqp", "hostname", "amqp.example.com")
    port = cp_get(cp, "amqp", "port", 5672)
    vhost = cp_get(cp, "amqp", "vhost", "/ois")

    credentials = pika.PlainCredentials(username, password)
    conn_params = pika.ConnectionParameters( credentials=credentials, host=hostname, port=port, virtual_host=vhost)
    connection = pika.BlockingConnection(conn_params)
    channel = connection.channel()

    return connection

def send_entries(channel, queue_name, updates):

    channel.queue_declare(queue=queue_name, durable=True)

    properties = pika.BasicProperties(
          #correlation_id="ce.grid.iu.edu"
          #delivery_mode = 2
        )

    for entry in updates:
        channel.basic_publish(exchange='', routing_key=queue_name, properties=properties, body=entry.to_ldif())


