
import pandas
import unittest
from unittest.mock import patch

from assmnt import ESMRData

class TestRuns(unittest.TestCase):

    @patch('assmnt.BytesIO', autospec=True)
    @patch('assmnt.ZipFile', autospec=True)
    @patch('assmnt.logger', autospec=True)
    def test_get_dltins_link(self, mock_logger, mock_zipfile, mock_bytesio):
        """
        success scenario for get_dltins_link() method
        """
        esmr = ESMRData()
        ESMRData.get_files_load(esmr)
        self.assertTrue(mock_logger.info.called, 'logger.info() not called')
        self.assertTrue(mock_zipfile.called, 'mock_zipfile() not called')
        self.assertFalse(mock_bytesio.called, 'Unexpected call to BytesIO()')

    @patch('assmnt.et', autospec=True)
    @patch('assmnt.logger', autospec=True)
    def test_parse_files(self, mock_logger, mock_etree):
        """
        success scenario for get_dltins_link() method
        """
        esmr = ESMRData()
        esmr.parse_files('dummy', 'dummy')
        self.assertTrue(mock_logger.info.called, 'logger.info() not called')
        self.assertTrue(mock_etree.iterparse.called, 'etree.iterparse() not called')


if __name__ == '__main__':
    unittest.main()
