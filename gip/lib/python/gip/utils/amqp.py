
import types

import pika
import simplejson

from gip_common import cp_get, cp_getInt, getLogger

import pika.adapters
import pika.adapters.select_connection
pika.adapters.select_connection.SELECT_TYPE = 'select'

log = getLogger("GIP.AMQP")

class PikaUpdater(object):

    """
    A wrapper on top of a Pika channel that handles the async I/O with timeouts
    """

    def __init__(self, connection_parameters, replaces, modifies):
        self.replaces = replaces
        self.modifies = modifies
        self.channel = None
        self.queue_name = None
        self.total_sent = 0
        self.total_responses = 0
        self.connection = pika.adapters.SelectConnection(parameters=connection_parameters, on_open_callback=self.on_connected)
        self.sent_entries = []

    def on_connected(self, connection):
        connection.channel(self.on_channel_open)

    def on_channel_open(self, new_channel):
        self.channel = new_channel
        self.channel.exchange_declare(exchange="gip", type="fanout", callback=self.on_exchange_declared)

    def on_exchange_declared(self, frame):
        self.channel.queue_declare(exclusive=True, callback=self.on_queue_declared)

    def on_queue_declared(self, frame):

        self.queue_name = frame.method.queue
        
        # Our exclusive queue has been created; send our commands.
        properties = pika.BasicProperties(
          #correlation_id=self.queue_name,
          reply_to=self.queue_name
          #delivery_mode = 2
        )

        total_sent = 0
        for entry in self.replaces:
            if 'GIPExpiration' in entry.nonglue:
                del entry.nonglue["GIPExpiration"]
            body = "replace" + entry.to_ldif()[2:]
            self.channel.basic_publish(exchange='gip', routing_key="",
                properties=properties, body=body)
            self.sent_entries.append(entry)
            total_sent += 1

        for entry in self.modifies:
            if 'GIPExpiration' in entry.nonglue:
                del entry.nonglue["GIPExpiration"]
            body_header = "modify: %s" % ", ".join(entry.dn)
            body_contents = []
            for attr, val in entry.glue.items():
                if isinstance(val, types.TupleType):
                    val = list(val)
                body_contents.append(["REPLACE", attr, val])
            for attr, val in entry.nonglue.items():
                if isinstance(val, types.TupleType):
                    val = list(val)
                body_contents.append(["REPLACE", attr, val])
            body = "%s\n%s" % (body_header, simplejson.dumps(body_contents))

            self.channel.basic_publish(exchange="gip", routing_key="",
                properties=properties, body=body)
            self.sent_entries.append(entry)
            total_sent += 1

        if total_sent:
            self.channel.basic_consume(self.handle_delivery, queue=self.queue_name)
        else:
            self.connection.close()

        self.total_sent = total_sent

    def handle_delivery(self, channel, method, header, body):
        self.total_responses += 1
        if (body != "modifysuccess") and (body != "replacesuccess"):
            log.warning("Failure from AMQP server: %s" % body.strip())
            log.warning("Failed LDAP:\n%s" % self.sent_entries[method.delivery_tag-1].to_ldif().strip())
        if self.total_responses == self.total_sent:
            self.connection.close()

    def run(self):
        try:
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            self.connection.close()
            # Pika docs indicate this is the correct post-close action.
            self.connection.ioloop.start()

def send_updates(cp, queue_name, replaces, modifies):

    if not replaces and not modifies:
        return

    username = cp_get(cp, "amqp", "user", "gip")
    # TODO: Haven't decided on the authorization model yet.  For now, we toss
    # around shared secrets in the gip.conf
    password = cp_get(cp, "amqp", "password", "gip")
    hostname = cp_get(cp, "amqp", "hostname", "amqp.example.com")
    port = cp_get(cp, "amqp", "port", 5672)
    vhost = cp_get(cp, "amqp", "vhost", "/ois")

    credentials = pika.PlainCredentials(username, password)
    conn_params = pika.ConnectionParameters(credentials=credentials, host=hostname, port=port, virtual_host=vhost)

    pu = PikaUpdater(conn_params, replaces, modifies)
    pu.run()

def send_modifies(channel, queue_name, entries):

    if not entries:
        return

    channel.exchange_declare(exchange="gip", type="fanout")

    properties = pika.BasicProperties(
          correlation_id=queue_name,
          #delivery_mode = 2
        )

    callback_queue = channel.queue_declare(exclusive=True)
    callback_queue_name = callback_queue.method.queue
    success_count = 0
    err_count = 0

    def callback(ch, method, props, body):
        print body
        #channel.stop_consuming()

    for entry in entries:
        if 'GIPExpiration' in entry.nonglue:
            del entry.nonglue["GIPExpiration"]
        body_header = "modify: %s" % ", ".join(entry.dn)
        body_contents = []
        for attr, val in entry.glue.items():
            if isinstance(val, types.TupleType):
                val = list(val)
            body_contents.append(["REPLACE", attr, val])
        for attr, val in entry.nonglue.items():
            if isinstance(val, types.TupleType):
                val = list(val)
            body_contents.append(["REPLACE", attr, val])
        body = "%s\n%s" % (body_header, simplejson.dumps(body_contents))

        properties = pika.BasicProperties(
            correlation_id=queue_name,
            reply_to=callback_queue_name,
            #delivery_mode = 2
        )

        channel.basic_publish(exchange="gip", routing_key="", properties=properties, body=body)

    channel.basic_consume(callback, no_ack=False, queue=callback_queue_name)
    channel.start_consuming()

