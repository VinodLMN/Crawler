
import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
from zipfile import ZipFile
import xml.etree.cElementTree as et
import pandas as pd
from datetime import datetime
from S3_loader import S3_loader
import custom_logger as clog

logger = clog.get_logger(__name__)

class ESMRData(object):
    '''
    class for downloading the latest updated xml file from website
    parsing file to create a dataframe
    moving the dataframe to s3 bucket
    '''

    def __init__(self):
        '''
        Constuctor for the class
        setting file name, link to main page and aws bucket
        '''
        self.fname = f"DLTINS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.link_xml = requests.get("https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq="
                            "publication_date:%5B2020-01-08T00:00:00Z+TO+2020-01-08T23:59:59Z%5D&wt=xml&indent=true"
                            "&start=0&rows=100")
        self.bucket = 'abc'

    def get_files_load(self):
        '''
        Get main page and filter out the recent zip file
        :return: zipfile and all files inside the zip
        '''
        soup = BeautifulSoup(self.link_xml.content, 'xml')
        # print(soup)
        link = soup.find("str", attrs={'name': "download_link"}, text=re.compile("DLTINS"))

        logger.info("Link to zip file: {}".format(link))
        zip_file = requests.get(link.text)
        zipfile = ZipFile(BytesIO(zip_file.content))
        return zipfile, zipfile.namelist()

    def parse_files(self, zipfile, zip_names):
        '''
        Parse xml file and generate the required dataframe
        :param zipfile: latest zip file
        :param zip_names: list of files inside zip file
        :return: dataframe with required fileds from xml file
        '''
        logger.info("Files in zip:{}".format(zip_names))
        for data_file in zip_names:
            if data_file.endswith('.xml'):
                # count = 0
                rows = []
                logger.info("Parsing xml file{}".format(data_file))
                for event, element in et.iterparse(zipfile.open(data_file), events=('start', 'end')):
                    data = dict()
                    _, _, element.tag = element.tag.rpartition('}')
                    # print(element.tag)
                    if element.tag == 'ModfdRcrd':
                        # print(element)
                        children1 = list(element)
                        for child in children1:
                            # print(child)
                            _, _, child.tag = child.tag.rpartition('}')
                            if child.tag == 'Issr':
                                data['Issr'] = child.text
                            if child.tag == 'FinInstrmGnlAttrbts':
                                children2 = list(child)
                                for child2 in children2:
                                    # print(child2)
                                    _, _, child2.tag = child2.tag.rpartition('}')
                                    if child2.tag == 'Id':
                                        data['Id'] = child2.text
                                    if child2.tag == 'FullNm':
                                        data['FullNm'] = child2.text
                                    if child2.tag == 'ClssfctnTp':
                                        data['ClssfctnTp'] = child2.text
                                    if child2.tag == 'CmmdtyDerivInd':
                                        data['CmmdtyDerivInd'] = child2.text
                                    if child2.tag == 'NtnlCcy':
                                        data['NtnlCcy'] = child2.text
                        rows.append(data)

                        # count += 1
                        # if count > 300:
                        #     break
                df = pd.DataFrame(rows)
                return df

    def run(self):
        try:
            logger.info("Get latest zip file from web page{}".format(self.link_xml))
            zipfile, files = self.get_files_load()
            logger.info("Parse the xml file and return a dataframe")
            df = self.parse_files(zipfile, files)
            logger.info("Rows in dataframe: {}".format(len(df)))
            logger.info("Upload the dataframe to AWS bucket{}".format(self.bucket))
            s3client = S3_loader()
            # s3client.copy_to_s3(df, self.bucket, self.fname)
        except Exception as e:
            logger.error('Something went wrong - {e.__str__()}')
            raise

if __name__ == '__main__':
    ESMRData().run()
