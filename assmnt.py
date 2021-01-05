
import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
from zipfile import ZipFile
import xml.etree.cElementTree as et
import pandas as pd
from datetime import datetime
from S3_loader import S3_loader

class ESMRData(object):

    def __init__(self):
        self.fname = f"DLTINS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.link_xml = requests.get("https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq="
                            "publication_date:%5B2020-01-08T00:00:00Z+TO+2020-01-08T23:59:59Z%5D&wt=xml&indent=true"
                            "&start=0&rows=100")
        self.bucket = 'abc'

    # print(data_xml.content)

    def getFilesToLoad(self):
        soup = BeautifulSoup(self.link_xml.content, 'xml')
        # print(soup)
        link = soup.find("str", attrs={'name': "download_link"}, text=re.compile("DLTINS"))

        print(link)
        zip_file = requests.get(link.text)
        zipfile = ZipFile(BytesIO(zip_file.content))
        return zipfile, zipfile.namelist()

    def parseFiles(self, zipfile, zip_names):
        for data_file in zip_names:
            if data_file.endswith('.xml'):
                count = 0
                rows = []
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

                        count += 1
                        if count > 3000:
                            break
                df = pd.DataFrame(rows)
                return df

    def run(self):
        zipfile, files = self.getFilesToLoad()
        df = self.parseFiles(zipfile, files)
        print(len(df))
        s3client = S3_loader()
        # s3client.copy_to_s3(df, self.bucket, self.fname)
        # pass


if __name__ == '__main__':
    ESMRData().run()
