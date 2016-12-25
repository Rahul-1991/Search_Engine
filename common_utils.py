from config import Config
import pika


class RabbitMQConnection(object):

    def get_rabbitmq_connection(self):
        url = Config.RABBITMQ.get('url')
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()
        return channel


def get_read_handle(filename):
    return open(filename, 'r')


def get_write_handle(filename):
    return open(filename, 'wa')


def close_file_handle(handle):
    handle.close()


