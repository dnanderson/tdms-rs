import unittest
import os
import numpy as np
import datetime
from nptdms import TdmsFile
from tdms.tdms_python import AsyncTdmsWriter, TdmsReader, DataType

class TestAsyncTdmsWriter(unittest.TestCase):
    def setUp(self):
        self.test_file = "test_async.tdms"
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def tearDown(self):
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_create_file(self):
        with AsyncTdmsWriter(self.test_file) as writer:
            pass
        self.assertTrue(os.path.exists(self.test_file))

    def test_write_f64_data(self):
        with AsyncTdmsWriter(self.test_file) as writer:
            data = np.array([1.0, 2.0, 3.0], dtype=np.float64)
            writer.create_channel("group", "channel", DataType.F64)
            writer.write_data("group", "channel", data)

        with TdmsFile.open(self.test_file) as tdms_file:
            channel = tdms_file["group"]["channel"]
            np.testing.assert_array_equal(channel[:], data)

    def test_write_i32_data(self):
        with AsyncTdmsWriter(self.test_file) as writer:
            data = np.array([-1, 0, 1], dtype=np.int32)
            writer.create_channel("group", "channel", DataType.I32)
            writer.write_data("group", "channel", data)

        with TdmsReader(self.test_file) as reader:
            read_data = reader.read_data("group", "channel")
            np.testing.assert_array_equal(read_data, data)

    def test_write_string_data(self):
        with AsyncTdmsWriter(self.test_file) as writer:
            data = ["hello", "world"]
            writer.create_channel("group", "channel", DataType.STRING)
            writer.write_strings("group", "channel", data)

        with TdmsReader(self.test_file) as reader:
            read_data = reader.read_strings("group", "channel")
            self.assertEqual(read_data, data)

    def test_write_timestamp_data(self):
        with AsyncTdmsWriter(self.test_file) as writer:
            data = np.array([
                np.datetime64('2023-01-01T12:00:00.123456'),
                np.datetime64('2023-01-02T13:00:00.654321')
            ], dtype='datetime64[ns]')
            writer.create_channel("group", "channel", DataType.TIMESTAMP)
            writer.write_data("group", "channel", data)

        with TdmsFile.open(self.test_file) as tdms_file:
            channel = tdms_file["group"]["channel"]
            read_data = channel[:]
            np.testing.assert_array_equal(read_data, data)

    def test_write_bool_data(self):
        with AsyncTdmsWriter(self.test_file) as writer:
            data = np.array([True, False, True], dtype=np.bool_)
            writer.create_channel("group", "channel", DataType.BOOLEAN)
            writer.write_data("group", "channel", data)

        with TdmsReader(self.test_file) as reader:
            read_data = reader.read_data("group", "channel")
            np.testing.assert_array_equal(read_data, data)

    def test_set_file_property(self):
        with AsyncTdmsWriter(self.test_file) as writer:
            writer.set_file_property("prop_name", "prop_value")
            writer.set_file_property("prop_int", 123)
            writer.set_file_property("prop_float", 1.23)
            writer.set_file_property("prop_bool", True)
            writer.set_file_property("prop_time", datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc))


        with TdmsFile.open(self.test_file) as tdms_file:
            self.assertEqual(tdms_file.properties["prop_name"], "prop_value")
            self.assertEqual(tdms_file.properties["prop_int"], 123)
            self.assertAlmostEqual(tdms_file.properties["prop_float"], 1.23)
            self.assertEqual(tdms_file.properties["prop_bool"], True)
            self.assertEqual(tdms_file.properties["prop_time"], np.datetime64('2023-01-01T12:00:00'))

if __name__ == '__main__':
    unittest.main()
