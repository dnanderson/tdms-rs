
import nptdms
import sys
import numpy as np

def verify_tdms_file(file_path):
    with nptdms.TdmsFile.open(file_path) as tdms_file:
        group = tdms_file["group"]
        channel = group["channel"]
        data = channel[:]
        expected_data = np.arange(0, 500, dtype=np.int32)
        assert np.array_equal(data, expected_data)

if __name__ == "__main__":
    file_path = sys.argv[1]
    verify_tdms_file(file_path)
