import unittest
import ConfigParser
import hadoopBenchmarker


class HadoopBenchmarkerTests(unittest.TestCase):
    """
    Tests for `hadoopBechmarker.py`
    """

    def test_constructCommandException(self):
        config = ConfigParser.ConfigParser()
        test_section = 'sampleTest'

        with self.assertRaises(Exception):
            config.remove_section(test_section)
            config.set(test_section, 'command', 'abc')
            config.set(test_section, 'tool', 'jar')
            hadoopBenchmarker.constructCommand(config, test_section)





if __name__ == '__main__':
    unittest.main()
