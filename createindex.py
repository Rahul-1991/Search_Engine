from xmlparser import XMLParser
from common_utils import *
import threading
import json
import re


class CreateIndex(object):

    def __init__(self, xml_file, stopwords_file):
        self.xmlfile = xml_file
        self.stopwordsfile = stopwords_file
        self.xmlparser = XMLParser()
        self.queue = Queue()
        self.block_dict = dict()

    def get_stopwords_dict_from_file(self):
        read_handle = get_read_handle(self.stopwordsfile)
        stopwords_list = [line.strip() for line in read_handle]
        read_handle.close()
        return dict.fromkeys(stopwords_list)

    def get_words(self, text):
        text = re.sub('[^0-9a-zA-Z]+', ' ', text)
        return text.split()

    def page_level_merge_dict(self, pageinfo_dict):
        parent_dict = dict()
        for field in pageinfo_dict.keys():
            if field == 'page_id':
                continue
            field_info = pageinfo_dict.get(field)
            words_in_field = self.get_words(field_info)
            for word in words_in_field:
                word_info = parent_dict.get(word, {'page_id': pageinfo_dict.get('page_id')})
                field_value = word_info.get(field, 0)
                word_info.update({field: field_value + 1})
                parent_dict.update({word: word_info})
        return parent_dict

    def add_pageinfo_to_block(self, word_info_dict):
        for word in word_info_dict:
            info_list_in_block = self.block_dict.get(word, [])
            info_list_in_block.append(word_info_dict.get(word))
            self.block_dict.update({word: info_list_in_block})

    def push_xml_page_to_queue(self):
        block_size = Config.BLOCKSIZE
        xml_root = self.xmlparser.get_xml_root(self.xmlfile)
        for page in xml_root.findall(Config.TAG):
            pageinfo_dict = self.xmlparser.parse_xml(page)
            page_info_by_word_dict = self.page_level_merge_dict(pageinfo_dict)
            self.add_pageinfo_to_block(page_info_by_word_dict)
            block_size -= 1
            if not block_size:
                block_size = Config.BLOCKSIZE
                self.queue.push_to_q(json.dumps(self.block_dict))
                self.block_dict = dict()
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
        self.file_count = 0

    def get_compressed_string(self, word_content):
        title = word_content.get('title')
        infobox = word_content.get('infobox')
        text = word_content.get('text')
        category = word_content.get('category')
        ext_links = word_content.get('ext_links')
        reference = word_content.get('reference')
        page_id = word_content.get('page_id')
        file_content = str(page_id)
        if title:
            file_content += 't' + str(title)
        if infobox:
            file_content += 'i' + str(infobox)
        if text:
            file_content += 'x' + str(text)
        if category:
            file_content += 'c' + str(category)
        if ext_links:
            file_content += 'e' + str(ext_links)
        if reference:
            file_content += 'r' + str(reference)
        return file_content

    def print_to_file(self, data, file_handle):
        file_content = ''
        for word in data:
            file_content += word + ' '
            word_content_list = data.get(word)
            for word_content in word_content_list:
                compressed_string = self.get_compressed_string(word_content)
                file_content += compressed_string + '|'
            file_content = file_content[:-1]
            file_content += '\n'
        file_handle.write(file_content)

    def push_to_q(self, data):
        self.connection.basic_publish(exchange='', routing_key='page_info_q', body=data)

    def pop_from_queue(self):
        def callback(ch, method, properties, body):
            print(" [x] Received ")
            write_handle = get_write_handle('index' + str(self.file_count))
            self.print_to_file(json.loads(body), write_handle)
            close_file_handle(write_handle)
            self.file_count += 1

        self.connection.basic_consume(callback, queue='page_info_q', no_ack=True)
        self.connection.start_consuming()

index = CreateIndex('sample.xml', 'stopwords-500.txt')
index.create_index()