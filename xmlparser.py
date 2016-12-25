import re
import xml.etree.ElementTree as ET


class XMLParser(object):

    def get_xml_root(self, filename):
        tree = ET.parse(filename)
        return tree.getroot()

    def get_title_from_page(self, page):
        title = page.find('title').text
        return title if title else None

    def get_id_from_page(self, page):
        page_id = page.find('id').text
        return page_id if page_id else None

    def get_text_from_page(self, page):
        revision = page.find('revision')
        page_text = revision.find('text').text
        return page_text if page_text else None

    def get_category_from_text(self, text):
        category_list = re.findall('Category:.*?\]\]', text, re.DOTALL)
        return ' '.join([category.strip(']') for category in category_list])

    def get_infobox_from_text(self, text):
        infobox_list = re.findall("Infobox(.*)\'\'\'", text, re.DOTALL)
        return ' '.join([content.strip('[]') for content in re.findall('\[\[.*?\]\]', infobox_list[0], re.DOTALL)]) if infobox_list else ''

    def get_reference_from_text(self, text):
        return ''

    def get_externallinks_from_text(self, text):
        externallink_list = re.findall('==[ ]*External links.*', text, re.DOTALL)
        return ''

    def parse_xml(self, page):
        title = self.get_title_from_page(page)
        page_id = self.get_id_from_page(page)
        text = self.get_text_from_page(page)
        if not title or not page_id or not text:
            return {}
        category = self.get_category_from_text(text)
        infobox = self.get_infobox_from_text(text)
        reference = self.get_reference_from_text(text)
        ext_links = self.get_externallinks_from_text(text)
        return {'title': title,
                'page_id': page_id,
                'text': text,
                'category': category,
                'infobox': infobox,
                'reference': reference,
                'ext_links': ext_links}
