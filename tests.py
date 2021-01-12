import pandas
import unittest
from unittest.mock import patch

from steeleye_assmnt import ESMRData

class TestRuns(unittest.TestCase):

    @patch('steeleye_assmnt.logger', autospec=True)
    def test_get_dltins_link(self, mock_logger):
        """
        success scenario for get_dltins_link() method
        """
        esmr = ESMRData()
        ESMRData.get_files_load(esmr)
        self.assertTrue(mock_logger.info.called, 'logger.info() not called')
