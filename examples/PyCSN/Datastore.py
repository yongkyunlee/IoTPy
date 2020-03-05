"""
A version of the file uploader - could be used for the same input as Pick.py
"""

import sys
import os
import datetime

sys.path.append(os.path.abspath("../../IoTPy/core"))
sys.path.append(os.path.abspath("../../IoTPy/agent_types"))
sys.path.append(os.path.abspath("../../IoTPy/helper_functions"))

from stream import Stream
from merge import zip_streams
from run import run
from print_stream import print_stream
from recent_values import recent_values
from op import map_window, filter_element, map_element
from split import split_element
from basics import f_mul

FILE_STORE_INTERVAL = 2

PHIDGETS_NOMINAL_DATA_INTERVAL_MS = 4
PHIDGETS_DECIMATION = 1


def make_filename(timestamp):
    """ Makes filename of datastore file for the given timestamp """
    filestore_naming = "%Y%m%dT%H%M%S"
    timestamp_datetime = datetime.datetime.fromtimestamp(timestamp).strftime(filestore_naming)
    sps = int (1000 / (PHIDGETS_NOMINAL_DATA_INTERVAL_MS * PHIDGETS_DECIMATION))
    return './phidgetsdata/' + str(sps) + '_' + timestamp_datetime + '.dat'


def write_to_datastore_file(nezt, file_ptr):
    """ Writes the input stream to the given file_ptr """
    accelerations = nezt[:3]
    sample_timestamp = nezt[3]
    file_ptr.write("%20.5f" % sample_timestamp + ' ' +
                   ' '.join("%10.7f" % x for x in accelerations)+'\n')


def datastore_file_manager(in_streams, out_streams):
    """ Agent that manages writing to and creating datastore files
    @in_streams - Stream of (n, e, z, t)
    @out_streams - Stream of (n, e, z, t) - stream itself is not modified
    """
    def write_to_file(stream, state):
        """ Writes stream to file. Creates a new file after FILE_STORE_INTERVAL passes.
        @param stream - input stream of acceleration data.
        @param state - tuple of (timestamp, curr_file_pointer, n_writing_error)
        """
        timestamp = stream[3]
        latest_timestamp, curr_file_ptr, curr_filename, n_writing_error = state
        # update the datastore file if FILE_STORE_INTERVAL passed
        if timestamp >= latest_timestamp + FILE_STORE_INTERVAL:
            if curr_file_ptr is not None:
                curr_file_ptr.close()
            curr_filename = make_filename(timestamp)
            curr_file_ptr = open(curr_filename, 'w')
            latest_timestamp = timestamp

        try:
            write_to_datastore_file(stream, curr_file_ptr)
        except:
            n_writing_error += 1
            if n_writing_error <= 10:
                print('Error writing sample to file {} with timestamp {}'.format(curr_filename, timestamp))

        return stream, (latest_timestamp, curr_file_ptr, curr_filename, n_writing_error)

    map_element(write_to_file, in_streams,
                out_streams, state=(-FILE_STORE_INTERVAL, None, None, 0))


def test_datastore_file_manager():
    nezt = Stream('nezt')
    nezt_out = Stream('nezt')
    datastore_file_manager(nezt, nezt_out)
    nezt.extend([
        (3.0, 3.0, 3.0, 1.0),
        (3.0, 3.0, 3.0, 2.0),
        (6.0, 12.0, 6.0, 3.0),
        (12.0, 24.0, 12.0, 4.0),
        (12.0, 24.0, 12.0, 5.0),
        (60.0, 120.0, 60.0, 6.0),
        (24.0, 48.0, 24.0, 7.0),
        (24.0, 48.0, 24.0, 8.0),
        (12.0, 24.0, 12.0, 9.0),
        (18.0, 36.0, 18.0, 10.0)])
    run()


if __name__ == '__main__':
    test_datastore_file_manager()