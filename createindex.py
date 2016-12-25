from xmlparser import XMLParser
from common_utils import *
import threading
import json
import re
from collections import defaultdict

class CreateIndex(object):

    def __init__(self, xml_file, stopwords_file):
        self.xmlfile = xml_file
        self.stopwordsfile = stopwords_file
        self.xmlparser = XMLParser()
        self.queue = Queue()
        self.block_dict = dict()

    @staticmethod
    def get_read_handle(filename):
        return open(filename, 'r')

    def get_stopwords_dict_from_file(self):
        read_handle = self.get_read_handle(self.stopwordsfile)
        stopwords_list = [line.strip() for line in read_handle]
        read_handle.close()
        return dict.fromkeys(stopwords_list)

    def get_words(self, text):
        text = re.sub('[^0-9a-zA-Z]+', ' ', text)
        return text.split()

    def page_level_merge_dict(self, pageinfo_dict):
        parent_dict = dict()
        for field in pageinfo_dict.keys():
            field_info = pageinfo_dict.get(field)
            words_in_field = self.get_words(field_info)
            for word in words_in_field:
                word_info = parent_dict.get(word, {})
                field_value = word_info.get(field, 0)
                word_info.update({field: field_value + 1})
                parent_dict.update({word: word_info})
        return parent_dict

    def block_level_merge_dict(self, word_info_dict):
        for word, word_info in word_info_dict.iteritems():
            word_info_from_block = self.block_dict.get(word, {})
            for field in word_info:
                word_info_from_block.update(
                    {field: word_info.get(field) + word_info_from_block.get(field, 0)})
            self.block_dict.update({word: word_info_from_block})

    def push_xml_page_to_queue(self):
        block_size = Config.BLOCKSIZE
        xml_root = self.xmlparser.get_xml_root(self.xmlfile)
        for page in xml_root.findall(Config.TAG):
            pageinfo_dict = self.xmlparser.parse_xml(page)
            page_info_by_word_dict = self.page_level_merge_dict(pageinfo_dict)
            self.block_level_merge_dict(page_info_by_word_dict)
        self.queue.push_to_q(json.dumps(self.block_dict))

    def get_page_from_queue(self):
        self.queue.pop_from_queue()

    def create_index(self):
        push_page_thread = threading.Thread(target=self.push_xml_page_to_queue)
        push_page_thread.daemon = True
        push_page_thread.start()
        write_to_file_thread = threading.Thread(target=self.get_page_from_queue())
        write_to_file_thread.daemon = True
        write_to_file_thread.start()


class Queue(object):

    def __init__(self):
        self.connection = RabbitMQConnection().get_rabbitmq_connection()

    def push_to_q(self, data):
        self.connection.basic_publish(exchange='', routing_key='page_info_q', body=data)

    def pop_from_queue(self):
        def callback(ch, method, properties, body):
            print(" [x] Received %r" % body)
            print_to_file(json.loads(body))
        self.connection.basic_consume(callback, queue='page_info_q', no_ack=True)
        self.connection.start_consuming()

index = CreateIndex('sample.xml', 'stopwords-500.txt')
index.create_index()