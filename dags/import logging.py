import logging
import xml.etree.ElementTree as ET
import csv


def xml_parser_func():
        parser = ET.XMLParser(encoding='UTF-8')
        tree = ET.parse('cbr.xml', parser=parser)
        root = tree.getroot()

        with open('cbr.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']]  + [NumCode] + [CharCode] + [Nominal] + 
                                [Name] + [Value.replace(',', '.')])
                logging.info([root.attrib['Date']] + [NumCode] + [CharCode] +  [Nominal] + 
                                [Name] + [Value.replace(',', '.')])


xml_parser_func()