"""
Copyright 2018 California Institute of Technology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
# coding=utf-8

"""Phidgets CSN Sensor"""
#import sys
import os
import errno
import shutil
import math

import datetime
import time

import logging
import threading
import Queue
import json

#import boto.sqs
#from boto.sqs.message import Message


from Phidgets import Phidget
from Phidgets.PhidgetException import PhidgetErrorCodes, PhidgetException
from Phidgets.Events.Events import AccelerationChangeEventArgs, AttachEventArgs, DetachEventArgs, ErrorEventArgs
from Phidgets.Devices.Spatial import Spatial, SpatialEventData, TimeSpan

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
from sink import sink_element

# For FinDer

G_TO_CMS2 = 981.0
TIMESTAMP_NAMING = "%Y-%m-%dT%H:%M:%S.%f"


RANDOM_WAIT_TIME = 10
MINIMUM_WAIT_TIME = 2

# our special order CSN Phidgets are scaled to +/- 2g instead of +/- 6g
PHIDGETS_ACCELERATION_TO_G = 1.0/3.0
# we request samples at 250 per second
PHIDGETS_NOMINAL_DATA_INTERVAL_MS = 4
PHIDGETS_NOMINAL_DATA_INTERVAL = 0.001 * PHIDGETS_NOMINAL_DATA_INTERVAL_MS
# We decimate to 50=5 or 250=1 samples per second
PHIDGETS_DECIMATION = 1

LTA = 10.0
STA = 0.5
Gap = 1.0
KSIGMA_THRESHOLD = 2.5

# picker interval is the expected interval between sample timestamps sent to the picker
PICKER_INTERVAL = 0.02
PICKER_THRESHOLD = 0.0025

RECENT_PICKS_COUNT = 100

MINIMUM_REPICK_INTERVAL_SECONDS = 1.0
PHIDGETS_RESAMPLED_DATA_INTERVAL_MS = PHIDGETS_NOMINAL_DATA_INTERVAL_MS * PHIDGETS_DECIMATION
FILESTORE_NAMING = "%Y%m%dT%H%M%S"

class PhidgetsSensor(object):

    def __init__(self, config=None):
        if not config:
            config = {}
        self.phidget_attached = False
        self.collect_samples = True
        self.picker = True
        self.sensor_data_lock = threading.Lock()

        self.delta_fields = set()
        self.sensor_data = {}
        self.sensor_data['Type'] = 'ACCELEROMETER_3_AXIS'
        self.sensor_data['Calibrated'] = True
        self.sensor_data['Model'] = 'Phidgets 1056'
        self.decimation = PHIDGETS_DECIMATION
        if 'software_version' in config:
            self.software_version = config.get('software_version')
        else:
            self.software_version = 'PyCSN Unknown'
        if 'decimation' in config:
            self.decimation = config.get('decimation')
        if 'picker' in config:
            self.picker = config.get('picker')
        if 'pick_threshold' in config:
            self.pick_threshold = config.get('pick_threshold')
        else:
            self.pick_threshold = PICKER_THRESHOLD
        if 'serial' in config:
            self.sensor_data['Serial'] = config.get('serial')
        else:
            self.sensor_data['Serial'] = ''
        if 'datastore' in config:
            self.datastore = config.get('datastore')
        else:
            self.datastore = '/var/tmp/phidgetsdata'
            
        #if 'pick_queue' in config:
        #    self.pick_queue = config.get('pick_queue')
        #else:
        #    self.pick_queue = None
            
        if 'latitude' in config:
            self.latitude = config.get('latitude')
        else:
            self.latitude = 0.0
        if 'longitude' in config:
            self.longitude = config.get('longitude')
        else:
            self.longitude = 0.0
        if 'floor' in config:
            self.floor = config.get('floor')
        else:
            self.floor = '1'
        
        if 'client_id' in config:
            self.client_id = config.get('client_id')
        else:
            self.client_id = 'Unknown'
            
        if 'stomp' in config:
            self.stomp = config.get('stomp')
        else:
            self.stomp = None
            
        if 'stomp_topic' in config:
            self.stomp_topic = config.get('stomp_topic')
        else:
            self.stomp_topic = None
            
        if 'connect_stomp' in config:
            self.connect_stomp = config.get('connect_stomp')
        else:
            self.connect_stomp = None
                    
        self.datastore_uploaded = self.datastore + '/uploaded'
        self.datastore_corrupted = self.datastore + '/corrupted'
        logging.info('Sensor datastore directory is %s',self.datastore)
            
        # Make sure data directory exists
        try:
            os.makedirs(self.datastore)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                logging.error('Error making directory %s', self.datastore)
                raise exception      
        try:
            os.makedirs(self.datastore_uploaded)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                logging.error('Error making directory %s', self.datastore_uploaded)
                raise exception
        try:
            os.makedirs(self.datastore_corrupted)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                logging.error('Error making directory %s', self.datastore_corrupted)
                raise exception            
         

        self.sensor_data['Units'] = 'g'
        self.sensor_data['Num_samples'] = 50
        self.sensor_data['Sample_window_size'] = 1
        self.accelerometer = None
        self.time_start = None
        self.reported_clock_drift = 0.0
        self.file_store_interval = 600.0
        self.last_phidgets_timestamp = None
        self.last_datastore_filename = None
        self.last_datastore_filename_uploaded = None
        self.writing_errors = 0
        self.last_sample_seconds = time.time()
        # a lock for updating the timing variables
        self.timing_lock = threading.Lock()
        
        self.timing_gradient = 0.0
        self.timing_intercept = 0.0
        self.timing_base_time = time.time()
        
        self.recent_picks = []
    
        # Everything looks OK, start the collect samples and picking threads and the Phidget
        """
        self.sensor_raw_data_queue = Queue.Queue()
        
        raw_data_thread = threading.Thread(target=self.ProcessSensorReading, args=['raw'])
        raw_data_thread.setDaemon(True)
        raw_data_thread.start()
        
        self.sensor_raw_data_queue.join()
        logging.info('Raw Data Thread started')
          
        # start the Picker thread, if this is required
        if self.picker:
            self.sensor_readings_queue = Queue.Queue()      
            thread = threading.Thread(target=self.Picker, args=['Simple Threshold'])
            thread.setDaemon(True)
            thread.start()
            logging.info('Picker started')        
            self.sensor_readings_queue.join()
            logging.info('Sensor readings thread started')
        else:
            logging.info('This Phidget will not calculate or send picks')
        """
        self.sensor_raw_data_queue = Queue.Queue()

        raw_data_thread = threading.Thread(target=self.Picker, args=['Simple Threshold'])
        raw_data_thread.setDaemon(True)
        raw_data_thread.start()
        logging.info('Picker started')

        self.sensor_raw_data_queue.join()

        logging.info('Client initialised: now starting Phidget')
        
        self.data_buffer = []
         
        # Start the Phidget       
        self.StartPhidget()
        
        if not self.phidget_attached:
            raise
                 
        self.sensor_data['Serial'] = str(self.accelerometer.getSerialNum())
       
        self.DisplayDeviceInfo()

    
    def StartPhidget(self):
        # Initialise the Phidgets sensor
      
        self.phidget_attached = False
        self.time_start = None
        
        try:
            if self.accelerometer:
                self.accelerometer.closePhidget()
            
            self.accelerometer = Spatial()
            self.accelerometer.setOnAttachHandler(self.AccelerometerAttached)
            self.accelerometer.setOnDetachHandler(self.AccelerometerDetached)
            self.accelerometer.setOnErrorhandler(self.AccelerometerError)
            self.accelerometer.setOnSpatialDataHandler(self.SpatialData)
            
            #self.accelerometer.enableLogging(Phidget.PhidgetLogLevel.PHIDGET_LOG_WARNING, None)
            #self.accelerometer.enableLogging(Phidget.PhidgetLogLevel.PHIDGET_LOG_VERBOSE, None)
           
            self.accelerometer.openPhidget()
            
            self.accelerometer.waitForAttach(10000)
            
            # set data rate in milliseconds (we will decimate from 4ms to 20ms later)
            # we now do this in the Attach handler              
            #self.accelerometer.setDataRate(PHIDGETS_NOMINAL_DATA_INTERVAL_MS)

        except RuntimeError as e:
            logging.error("Runtime Exception: %s", e.details)
            return
        except PhidgetException as e:
            logging.error("Phidget Exception: %s. Is the Phidget not connected?", e.details)
            return
        
        self.phidget_attached = True


    def Picker(self, args):
        logging.info('Initialise picker %s', args)
        delta = PHIDGETS_NOMINAL_DATA_INTERVAL_MS * self.decimation * 0.001
        LTA_count = int(LTA / delta)

        def pick_orientation(scaled, timestamps, orientation):
            """ Sends picks on a single orientation, either 'n', 'e', or 'z'. """
            # ---------------------------------------------------------------
            # CREATE AGENTS AND STREAMS
            # ---------------------------------------------------------------

            # 1. DECIMATE SCALED DATA.
            # Window of size DECIMATION is decimated to its average.
            decimated = Stream('decimated')
            map_window(lambda v: sum(v) / float(len(v)), scaled, decimated,
                       window_size=self.decimation, step_size=self.decimation)

            # 2. DECIMATE TIMESTAMPS.
            # Window of size DECIMATION is decimated to its last value.
            decimated_timestamps = Stream('decimated_timestamps')
            map_window(lambda window: window[-1],
                       timestamps, decimated_timestamps,
                       window_size=self.decimation, step_size=self.decimation)

            # 3. DEMEAN (subtract mean from) DECIMATED STREAM.
            # Subtract mean of window from the window's last value.
            # Move sliding window forward by 1 step.
            demeaned = Stream('demeaned', initial_value=[0.0] * (LTA_count - 1))
            map_window(lambda window: window[-1] - sum(window) / float(len(window)),
                       decimated, demeaned,
                       window_size=LTA_count, step_size=1)

            # 4. MERGE TIMESTAMPS WITH DEMEANED ACCELERATIONS.
            # Merges decimated_timestamps and demeaned to get timestamped_data.
            timestamped_data = Stream('timestamped_data')
            zip_streams(in_streams=[decimated_timestamps, demeaned], out_stream=timestamped_data)

            # 5. DETECT PICKS.
            # Output a pick if the value part of the time_value (t_v) exceeds threshold.
            picks = Stream('picks')
            filter_element(lambda t_v: abs(t_v[1]) > self.pick_threshold, timestamped_data, picks)

            # 6. QUENCH PICKS.
            # An element is a (timestamp, value).
            # Start a new quench when timestamp > QUENCH_PERIOD + last_quench.
            # Update the last quench when a new quench is initiated.
            # Initially the last_quench (i.e. state) is 0.
            quenched_picks = Stream('quenched_picks')

            # f is the filtering function
            def f(timestamped_value, last_quench, QUENCH_PERIOD):
                timestamp, value = timestamped_value
                new_quench = timestamp > QUENCH_PERIOD + last_quench
                last_quench = timestamp if new_quench else last_quench
                # return filter condition (new_quench) and next state (last_quench)
                return new_quench, last_quench

            filter_element(f, picks, quenched_picks, state=0, QUENCH_PERIOD=2)

            # 7. SEND QUENCHED PICKS.
            self.send_event(quenched_picks)

        def picker_agent(nezt):
            # nezt is a stream of 'data_entry' in the original code.
            # Declare acceleration streams in n, e, z orientations and timestamp stream.
            n, e, z, t = (Stream('raw_north'), Stream('raw_east'), Stream('raw_vertical'),
                          Stream('timestamps'))

            # split the nezt stream into its components
            split_element(lambda nezt: [nezt[0], nezt[1], nezt[2], nezt[3]],
                          in_stream=nezt, out_streams=[n, e, z, t])

            # Determine picks for each orientation after scaling. Note negative scale for East.
            # Parameters of pick_orientation are:
            # (0) stream of accelerations in a single orientation, scaled by multiplying by SCALE.
            # (1) timestamps
            # (2) The orientation 'n', 'e', or 'z'
            pick_orientation(f_mul(n, PHIDGETS_ACCELERATION_TO_G), t, 'n')
            pick_orientation(f_mul(e, -PHIDGETS_ACCELERATION_TO_G), t, 'e')
            pick_orientation(f_mul(z, PHIDGETS_ACCELERATION_TO_G), t, 'z')

        def datastore_file_agent(in_streams, out_streams):
            """ Agent that manages writing to and creating datastore files
            @in_streams - Stream of (n, e, z, t)
            @out_streams - Stream of (n, e, z, t) - stream itself is not modified
            """

            def write_to_datastore_file(nezt, file_ptr):
                """ Writes the input stream to the given file_ptr """
                accelerations = nezt[:3]
                sample_timestamp = nezt[3]
                file_ptr.write("%20.5f" % sample_timestamp + ' ' +
                               ' '.join("%10.7f" % x for x in accelerations) + '\n')

            def write_to_file(stream, state):
                """ Writes stream to file. Creates a new file after FILE_STORE_INTERVAL passes.
                @param stream - input stream of acceleration data.
                @param state - tuple of (timestamp, curr_file_pointer, n_writing_error)
                """
                timestamp = stream[3]
                latest_timestamp, curr_file_ptr, curr_filename, n_sample_cnt, n_writing_error = state
                # update the datastore file if FILE_STORE_INTERVAL passed
                if timestamp >= latest_timestamp + self.file_store_interval:
                    logging.info('File %s store interval elapsed with %s samples, rate %s samples/second',
                                 curr_filename, n_sample_cnt, float(n_sample_cnt) / self.file_store_interval)
                    if curr_file_ptr is not None:
                        curr_file_ptr.close()
                    curr_filename = self.MakeFilename(timestamp)
                    curr_file_ptr = open(curr_filename, 'w')
                    latest_timestamp = timestamp
                    n_sample_cnt = 0

                try:
                    write_to_datastore_file(stream, curr_file_ptr)
                    n_sample_cnt += 1
                except:
                    n_writing_error += 1
                    if n_writing_error <= 10:
                        print('Error writing sample to file {} with timestamp {}'.format(curr_filename, timestamp))

                return stream, (latest_timestamp, curr_file_ptr, curr_filename, n_sample_cnt, n_writing_error)

            sink_element(func=write_to_file, in_stream=in_streams, state=(-self.file_store_interval, None, None, 0, 0))

        nezt = Stream('nezt')
        picker_agent(nezt)
        datastore_file_agent(nezt)

        while True:
            e, sample_timestamp = self.sensor_raw_data_queue.get()

            if self.collect_samples:
                if self.time_start is None:
                    self.time_start = sample_timestamp
                    self.last_phidgets_timestamp = 0.0

                for index, spatialData in enumerate(e.spatialData):
                    phidgets_timestamp = spatialData.Timestamp.seconds + (spatialData.Timestamp.microSeconds * 0.000001)
                    stream_data = (spatialData.Acceleration[1], spatialData.Acceleration[0],
                                   spatialData.Acceleration[2], sample_timestamp)
                    nezt.extend([stream_data])

                    if self.last_phidgets_timestamp:
                        sample_increment = int(round( (phidgets_timestamp - self.last_phidgets_timestamp) / PHIDGETS_NOMINAL_DATA_INTERVAL))
                        if sample_increment > 4*self.decimation:
                            logging.warn('Missing >3 samples: last sample %s current sample %s missing samples %s',\
                                         self.last_phidgets_timestamp, phidgets_timestamp, sample_increment)
                        elif sample_increment == 0:
                            logging.warn('Excess samples: last sample %s current sample %s equiv samples %s',\
                                         self.last_phidgets_timestamp, phidgets_timestamp, sample_increment)

                    self.last_phidgets_timestamp = phidgets_timestamp

    def DisplayDeviceInfo(self):
            print("|------------|----------------------------------|--------------|------------|")
            print("|- Attached -|-              Type              -|- Serial No. -|-  Version -|")
            print("|------------|----------------------------------|--------------|------------|")
            print("|- %8s -|- %30s -|- %10d -|- %8d -|" % (self.accelerometer.isAttached(), self.accelerometer.getDeviceName(), self.accelerometer.getSerialNum(), self.accelerometer.getDeviceVersion()))
            print("|------------|----------------------------------|--------------|------------|")
            print("Number of Axes: %i" % (self.accelerometer.getAccelerationAxisCount()))
            print('Max Acceleration Axis 0: {} Min Acceleration Axis 0: {}'.format(self.accelerometer.getAccelerationMax(0), self.accelerometer.getAccelerationMin(0)))
            print('Max Acceleration Axis 1: {} Min Acceleration Axis 1: {}'.format(self.accelerometer.getAccelerationMax(1), self.accelerometer.getAccelerationMin(1)))
            print('Max Acceleration Axis 2: {} Min Acceleration Axis 2: {}'.format(self.accelerometer.getAccelerationMax(2), self.accelerometer.getAccelerationMin(2)))
    

    def setFileStoreInterval(self, file_store_interval):
        # sets the interval for writing the data to a new file
        self.file_store_interval = file_store_interval
    
    #Event Handler Callback Functions
    def AccelerometerAttached(self, e):
        attached = e.device
        self.phidget_attached = True
        logging.info("Accelerometer %s Attached!", attached.getSerialNum())
        # set data rate in milliseconds (we will decimate from 4ms to 20ms later)
        self.accelerometer.setDataRate(PHIDGETS_NOMINAL_DATA_INTERVAL_MS)
        logging.info("Phidget data rate interval set to %s milliseconds", PHIDGETS_NOMINAL_DATA_INTERVAL_MS)

    
    def AccelerometerDetached(self, e):
        detached = e.device
        self.phidget_attached = False
        logging.error('Accelerometer %s Detached!',(detached.getSerialNum()))
            
    def AccelerometerError(self, e):
        try:
            source = e.device
            logging.error("Accelerometer %s: Phidget Error %s: %s", source.getSerialNum(), e.eCode, e.description)
        except PhidgetException as e:
            logging.error("Phidget Exception %s: %s", e.code, e.details)  
            
    def AccelerometerAccelerationChanged(self, e):
        source = e.device
        logging.error("Accelerometer %s: Axis %s: %s", source.getSerialNum(), e.index, e.acceleration)

    def MakeFilename(self, timestamp):
        timestamp_datetime = datetime.datetime.fromtimestamp(timestamp).strftime(FILESTORE_NAMING)
        sps = int (1000 / (PHIDGETS_NOMINAL_DATA_INTERVAL_MS * self.decimation))
        return self.datastore + '/' + str(sps) + '_' + timestamp_datetime + '.dat'

    def setTimingFitVariables(self, base_time, gradient, intercept):
        # this is called by the main client which is monitoring the system clock compared with NTP
        # we acquire a lock on the timing variables to update them
        with self.timing_lock:
            self.timing_base_time = base_time
            self.timing_gradient = gradient
            self.timing_intercept = intercept


    def GetNtpCorrectedTimestamp(self):
        # from the current system time we use the NTP thread's line fit to estimate the true time
        time_now = time.time()
        # we ensure that the timing variables are not being updated concurrently by acquiring a lock on them
        with self.timing_lock:
            offset_estimate = (time_now-self.timing_base_time) * self.timing_gradient + self.timing_intercept       
        return time_now + offset_estimate

    def StopSampleCollection(self):
        # This stops more samples being collected, and closes the current samples file
        self.collect_samples = False
        self.datastore_file.close()

    def StartSampleCollection(self):
        # This restarts sample collection (into files)
        self.collect_samples = True
        
    def SpatialData(self, e):
               
        if not self.phidget_attached:
            return
        
        sample_timestamp = self.GetNtpCorrectedTimestamp()
        
        self.sensor_raw_data_queue.put((e, sample_timestamp))

    def to_dict(self):
        # No need to persist anything that doesn't change.
        details = {}
        details['module'] = 'PhidgetsSensor'
        details['class'] = 'PhidgetsSensor'
        with self.sensor_data_lock:
            details['sensor_id'] = self.sensor_data
            details['serial'] = self.sensor_data['Serial']
        details['datastore'] = self.datastore
        details['decimation'] = self.decimation
        details['picker'] = self.picker
        details['pick_threshold'] = self.pick_threshold
        if self.last_datastore_filename_uploaded:
            details['last_upload'] = self.last_datastore_filename_uploaded
        else:
            details['last_upload'] = ''

        return details

    def get_metadata(self):
        logging.error('PhidgetSensor get_metadata Not implemented')

        
    def datafiles_not_yet_uploaded(self):
        # Returns a list of files that are older than last_datastore_filename (or current time) in the
        # data file directory that have not yet been uploaded
        # we subtract 10 minutes off the time to avoid finding the currently open file (although we could in 
        # principle trap that)
        
        file_list = []
        last_file_date = self.GetNtpCorrectedTimestamp() - 600.0
            
        logging.info('Searching for files older than %s',last_file_date)
        for f in os.listdir(self.datastore):
            filename = self.datastore + '/' + f
            if filename == self.datastore_filename:
                logging.info('Will not add currently opened file %s', filename)
                continue
            if os.path.isfile(filename):
                    try:
                        #t = os.path.getctime(filename)
                        file_date = os.path.getctime(filename)
                        if file_date < last_file_date:
                            if len(file_list) < 20:
                                logging.info('Not yet uploaded: %s',filename)
                            elif len(file_list) == 20:
                                logging.info('Not yet uploaded: %s (will not show more)', filename)
                            file_list.append(filename)
                    except:
                        logging.error('Error getting file time for %s', filename)
                
        return file_list
 
    def mark_file_uploaded(self, filename):
        # when a sensor data file has been successfully uploaded to the server we move it to the uploaded directory
        try:            
            shutil.copy2(filename, self.datastore_uploaded)
            self.last_datastore_filename_uploaded = filename
            os.remove(filename)
        except:
            logging.warning('Failed to move %s to %s', filename, self.datastore_uploaded)
        
    def mark_file_corrupted(self, filename):
        # when a sensor data file fails to convert to stream we move it to the corrupt directory
        try:            
            shutil.copy2(filename, self.datastore_corrupted)
        except:
            logging.warning('Failed to move %s to %s', filename, self.datastore_corrupted)
        # always remove the corrupt file from the main directory  
        try:     
            os.remove(filename)
        except:
            logging.error('Failed to delete %s', filename)
            
        

    def set_sensor_id(self, sensor_id):
        with self.sensor_data_lock:
            self.sensor_data['sensor_id'] = sensor_id


    def send_event(self, in_stream):
        
        """
        pick_message = {'timestamp': event_time,
                        'station': self.client_id,
                        'latitude': self.latitude,
                        'longitude': self.longitude,
                        'demeaned_accelerations': values,
                        'floor': self.floor,
                        }

        try:
        
            message_body = json.dumps(pick_message)
            
            m = Message()
            m.set_body(message_body)
            self.pick_queue.write(m)
            
            #logging.info('Sent pick to Amazon SQS: %s', pick_message)
            
        except Exception, e:
            logging.error('Failed to send pick message: %s', e)
        """
        def send_pick_to_finder(values):
            # values: (time stamp, accs)
            # Send pick to FinDer
            station_name = self.client_id
            station_name = station_name[0:1] + station_name[3:]
            finder_accs = [acc*G_TO_CMS2 for acc in values[1:]]
            channel = 'HNN'
            if finder_accs[1] > 0.0: channel = 'HNE'
            if finder_accs[2] > 0.0: channel = 'HNZ'
            pick_datetime = datetime.datetime.utcfromtimestamp(float(values[0]))
            pick_time = pick_datetime.strftime(TIMESTAMP_NAMING)
            finder_location = str(self.latitude) + ' ' + str(self.longitude)
            timenow_timestamp = self.GetNtpCorrectedTimestamp()
            timenow = datetime.datetime.utcfromtimestamp(timenow_timestamp)
            activemq_message = '1 ' + timenow.strftime(TIMESTAMP_NAMING)[:-3] + ' '+self.software_version+'\n'
            line = '%s CSN.%s.%s.-- %s %s %s %s\n' % \
                    (finder_location, station_name, channel, pick_time, abs(finder_accs[0]), abs(finder_accs[1]), abs(finder_accs[2]))
            activemq_message += line
            activemq_message += 'ENDOFDATA\n'
            logging.info("ActiveMQ message to send:\n%s", activemq_message[:-1])
            try:
                self.stomp.put(activemq_message, destination=self.stomp_topic)
            except Exception, err:
                logging.error('Error sending pick to ActiveMQ %s', err)
                logging.info('Trying to reconnect to ActiveMQ broker')
                self.stomp = self.connect_stomp()

        sink_element(func=send_pick_to_finder, in_stream=in_stream)

