from config import Config
import pika

class RabbitMQConnection(object):

    def get_rabbitmq_connection(self):
        url = Config.RABBITMQ.get('url')
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()
        return channel


def print_to_file(data):
    pass
