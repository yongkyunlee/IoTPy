"""
This module consists of sources. A source is a function that puts
data from a data source, such as a sensor, into a stream.

In addition, a function in this module wait for data to arrive
at a queue, and when data arrives it is put on a stream.
Another function puts data from a function call into a queue.

Each source function is executed in a separate thread. A
function returns a 2-tuple: (1) thread and (2) ready, a
threading.Event. ready.wait() waits until the thread is ready for
execution.

Functions in the module:
   1. func_to_q: puts data called by a function into a queue.
   2. q_to_streams: Used in ../multiprocessing
   3. q_to_streams_general: Used in ../multiprocessing
   4. source_function: puts data generated by a function into the
        queue read by the scheduler.
   5. source_file: puts data from a file into the queue read by the
        scheduler.
   6. source_list: puts data from a list into the queue read by the
        scheduler. 
    

"""
import time
import threading

import sys
import os
sys.path.append(os.path.abspath("../helper_functions"))
sys.path.append(os.path.abspath("../core"))
sys.path.append(os.path.abspath("../agent_types"))
from check_agent_parameter_types import check_source_function_arguments
from check_agent_parameter_types import check_source_file_arguments
from recent_values import recent_values
from stream import Stream
    
def func_to_q(func, q, state=None, sleep_time=0, num_steps=None,
              name='source_to_q', *args, **kwargs):
    """
    Value returned by func is appended to the queue q

    ----------
        func: function on state, args, kwargs
           Value returned by func is appended to the queue q
        q: Queue.Queue() or multiprocessing.Queue()
        sleep_time: int or float (optional)
           The thread sleeps for this amount of time (seconds) before
           successive calls to func.
        num_steps: int (optional)
           The number of calls to func before this thread
           terminates. If num_steps is None then this thread does not
           terminate.
        name: str (optional)
           Name of the string is helpful in debugging.
    Returns
    -------
        thread: threading.Thread()
            The thread that repeatedly calls function.
        ready: threading.Event()
            ready is set to signal that the thread is ready for
            execution. 
           
    """
    
    ready = threading.Event()
    ready.set()

    def thread_target(func, q, state, sleep_time, num_steps, args, kwargs):
        if num_steps is None:
            while True:
                output = func(*args, **kwargs) if state is None else \
                  func(state, *args, **kwargs)
                q.put(output)
                time.sleep(sleep_time)
        else:
            for _ in range(num_steps):
                output = func(*args, **kwargs) if state is None else \
                  func(state, *args, **kwargs)
                q.put(output)
                time.sleep(sleep_time)

    return (
        threading.Thread(
        target=thread_target,
        args=(func, q, state, sleep_time, num_steps, args, kwargs)),
        ready)
                

def q_to_streams(q, streams, name='thread_q_to_streams'):
    """
    Parameters
    ----------
    q: Queue.Queue or multiprocessing.Queue
       messages arriving on q are tuples (stream_name, message content).
       The message content is appended to the stream with the specified
       name
    streams: list of Stream
       Each stream in this list must have a unique name. When a message
       (stream_name, message_content) arrives at the queue,
       message_content is placed on the stream with name stream_name.
    name: str (optional)
       The name of this thread. Useful for debugging.

    Variables
    ---------
    name_to_stream: dict. key: stream_name. value: stream
    ready: threading.Event. Set to indicate thread is ready.
       
    """
    name_to_stream = {stream.name:stream for stream in streams}
    ready = threading.Event()
    def thread_target(q, name_to_stream):
        ready.set()
        while True:
            v = q.get()
            if v == '_close':
                break
            stream_name, new_data_for_stream = v
            stream = name_to_stream[stream_name]
            stream.append(new_data_for_stream)
        return
    
    return (
        threading.Thread(target=thread_target, args=(q, name_to_stream)),
        ready)


def q_to_streams_general(q, func, name='thread_q_to_streams'):
    """
    Identical to q_to_streams except that a stream is identified by the
    descriptor attached to each message in the queue. The descriptor need
    not be a name; for instance, the descriptor could be a 2-tuple
    consisting of (1) the name of an array of streams and (2) an index
    into the array. Note that for q_to_streams the descriptor must be a
    name.

    func is a function that returns a stream given a stream
    descriptor. In the case of q_to_streams we don't need func because
    the stream is identified by its name.
    
    Parameters
    ----------
    q: Queue.Queue or multiprocessing.Queue
       messages arriving on q are tuples (stream_name, message content).
       The message content is appended to the stream with the specified
       name
    func: function
       function from stream_descriptor to a stream
    name: str (optional)
       The name of this thread. Useful for debugging.

    Variables
    ---------
    ready: threading.Event. Set to indicate thread is ready.
       
    """
    ready = threading.Event()
    def thread_target(q, name_to_stream):
        ready.set()
        while True:
            v = q.get()
            if v == '_close':
                break
            # Each message is a tuple: (stream descriptor, msg content)
            stream_descriptor, new_data_for_stream = v
            stream = func(stream_descriptor)
            stream.append(new_data_for_stream)
        return
    return (
        threading.Thread(target=thread_target, args=(q, name_to_stream)),
        ready)


def source_to_stream(
        func, out_stream, time_interval=0, num_steps=None, window_size=1,
        state=None, name='source_to_stream', *args, **kwargs):
    """
    Calls func and places values returned by the function on out_stream.
    
    Parameters
    ----------
       func: function on state, args, kwargs
          This function is called and the result of this function must be
          a list; out_stream is extended by this list.
       out_stream: Stream
          The stream to which elements will be appended.
       time_interval: float or int (optional), time in seconds
          An element is placed on the output stream every time_interval
          seconds.
       num_steps: int (optional)
          source_fun terminates after num_steps: the number of steps.
          At each step a window_size number of elements is placed on the
          out_stream.
          If num_steps is None then source_fun terminates
          only when an exception is raised.
       window_size: int (optional)
          At each step a window_size number of elements is placed on the
          out_stream.
       state: object (optional)
          The state of the function; an argument of the parameter func.
       name: str or picklable, i.e., linearizable, object (optional)
           The name of the thread. Useful in debugging.
       args: list
          Positional arguments of func
       kwargs: dict
          Keyword arguments of func

    Returns: (thread, ready)
    -------
          thread: threading.Thread
             The thread created by this function. The thread must
             be started and thread.join() may have to be called to
             ensure that the thread terminates execution.
          ready: threading.Event
             Signals (sets the event, ready) when the thread is
             ready to operate.
          name: str
             The name of the thread
    """
    ready = threading.Event()

    def thread_target(
            func, out_stream, time_interval, ready,
            num_steps, window_size, state, args, kwargs):
        """
        thread_target is the function executed by the thread.
        """

        #-----------------------------------------------------------------
        def get_output_list_and_next_state(state):
            """
            This function returns a list of length window_size and the
            next state.

            Parameters
            ----------
               state is the current state of the agent.
            Returns
            -------
               output_list: list
                 list of length window_size
               next_state: object
                 The next state after output_list is created.

            """
            output_list = []
            next_state = state
            # Compute the output list of length window_size
            for _ in range(window_size):
                if next_state is not None:
                    # func has a single argument, state,
                    # apart from *args, **kwargs
                    output_increment, next_state = func(
                        next_state, *args, **kwargs)
                else:
                    # func has no arguments apart from
                    # *args, **kwargs
                    output_increment = func(*args, **kwargs)
                output_list.append(output_increment)
            # Finished computing output_list of length window_size
            return output_list, next_state
        #-----------------------------------------------------------------
        # End of def get_output_list_and_next_state(state)
        #-----------------------------------------------------------------

        # The thread event, ready, is set to indicate that this thread
        # is ready.
        ready.set()
        if num_steps is None:
            while True:
                output_list, state = get_output_list_and_next_state(state)
                out_stream.extend(output_list)
                time.sleep(time_interval)
        else:
            for _ in range(num_steps):
                output_list, state = get_output_list_and_next_state(state)
                out_stream.extend(output_list)
                time.sleep(time_interval)
        return
    #------------------------------------------------------------------------
    # End of def thread_target(...)
    #------------------------------------------------------------------------

    return (
        threading.Thread(
            target=thread_target,
            args=(func, out_stream, time_interval, ready, num_steps,
                  window_size, state, args, kwargs)),
        ready)


def source_function(
        func, stream, time_interval=0, num_steps=None, window_size=1,
        state=None, name='source_f', *args, **kwargs):
    """
    Identical to source_to_stream except that values from the source
    are put on the scheduler queue rather than on a stream.
    
    Calls func and places values returned by the function, with
    stream_name, on the scheduler queue. The scheduler then takes
    these values and appends them to the queue with the specified name.
    
    Parameters
    ----------
       func: function on state, args, kwargs
          This function is called and the 2-tuple consisting of (1)
          the result of this function and (2) stream_name is
          appended to the queue of the scheduler.
       stream: Stream
          The stream to which elements will be appended.
       time_interval: float or int (optional), time in seconds
          An element is placed on the output stream every time_interval
          seconds.
       num_steps: int (optional)
          source_fun terminates after num_steps: the number of steps.
          At each step a window_size number of elements is placed on the
          out_stream.
          If num_steps is None then source_fun terminates
          only when an exception is raised.
       window_size: int (optional)
          At each step a window_size number of elements is placed on the
          out_stream.
       state: object (optional)
          The state of the function; an argument of the parameter func.
       name: str or picklable, i.e., linearizable, object (optional)
           The name of the thread. Useful in debugging.
       args: list
          Positional arguments of func
       kwargs: dict
          Keyword arguments of func

    Returns: (thread, ready)
    -------
          thread: threading.Thread
             The thread created by this function. The thread must
             be started and thread.join() may have to be called to
             ensure that the thread terminates execution.
          ready: threading.Event
             Signals (sets the event, ready) when the thread is
             ready to operate.
          name: str
             The name of the thread
    """
    stream_name = stream.name
    check_source_function_arguments(
        func, stream_name, time_interval, num_steps, window_size,
        state, name)
    scheduler = Stream.scheduler
    ready = threading.Event()

    def thread_target(
            func, stream_name, time_interval, ready,
            num_steps, window_size, state, args, kwargs):
        """
        thread_target is the function executed by the thread.
        """

        #-----------------------------------------------------------------
        def get_output_list_and_next_state(state):
            """
            This function returns a list of length window_size and the
            next state.

            Parameters
            ----------
               state is the current state of the agent.
            Returns
            -------
               output_list: list
                 list of length window_size
               next_state: object
                 The next state after output_list is created.

            """
            output_list = []
            next_state = state
            # Compute the output list of length window_size
            for _ in range(window_size):
                if next_state is not None:
                    # func has a single argument, state,
                    # apart from *args, **kwargs
                    output_increment, next_state = func(
                        next_state, *args, **kwargs)
                else:
                    # func has no arguments apart from
                    # *args, **kwargs
                    output_increment = func(*args, **kwargs)
                output_list.append(output_increment)
            # Finished computing output_list of length window_size
            return output_list, next_state
        #-----------------------------------------------------------------
        # End of def get_output_list_and_next_state(state)
        #-----------------------------------------------------------------

        # The thread event, ready, is set to indicate that this thread
        # is ready.
        ready.set()
        if num_steps is None:
            while True:
                output_list, state = get_output_list_and_next_state(state)
                for v in output_list:
                    scheduler.input_queue.put((stream_name, v))
                time.sleep(time_interval)
        else:
            for _ in range(num_steps):
                output_list, state = get_output_list_and_next_state(state)
                for v in output_list:
                    scheduler.input_queue.put((stream_name, v))
                time.sleep(time_interval)
        return

    #------------------------------------------------------------------------
    # End of def thread_target(...)
    #------------------------------------------------------------------------

    return (
        threading.Thread(
            target=thread_target,
            args=(func, stream_name, time_interval, ready, num_steps,
              window_size, state, args, kwargs)),
            ready)



def source_file(
        func, stream, filename, time_interval=0,
        num_steps=None, window_size=1, state=None, name='source_file',
        *args, **kwargs):
    
    """
    Applies function func to each line in the file with name filename and
    puts the result of func on stream.
    
    Parameters
    ----------
       func: function
          This function is applied to each line read from file with name
          filename, and the result of this function is appended to stream.
          The arguments of func are a line of the file, the
          state if any, and *args, **kwargs.
       stream: The output stream to which elements are appended.
       filename: str
          The name of the file that is read.
       time_interval: float or int (optional)
          The next line of the file is read every time_interval seconds.
       num_steps: int (optional)
          file_to_stream terminates after num_steps taken.
          If num_steps is None then the
          file_to_stream terminates when the entire file is read.
        window_size: int (optional)
          At each step, window_size number of lines are read from the
          file and placed on out_stream after function is applied to them.
       state: object (optional)
          The state of the function; an argument of func.
       args: list
          Positional arguments of func
       kwargs: dict
          Keyword arguments of func
    
    Returns: (thread, ready)
    -------
          thread: threading.Thread
             The thread created by this function. The thread must
             be started and thread.join() may have to be called to
             ensure that the thread terminates execution.
          ready: threading.Event
             Signals (sets the event, ready) when the thread is
             ready to operate.
          name: str
             The name of the thread The thread must
             be started and thread.join() may have to be called to
             ensure that the thread terminates execution.

    """

    #-----------------------------------------------------------------
    stream_name = stream.name
    check_source_file_arguments(
        func, stream_name, filename, time_interval,
        num_steps, window_size, state, name)
    scheduler = Stream.scheduler
    ready = threading.Event()

    def thread_target(func, stream_name, filename, time_interval,
                      num_steps, window_size, state, ready,
                      args, kwargs):
        """
        This is the function executed by the thread.
        
        """
        num_lines_read_in_current_window = 0
        num_steps_taken = 0
        output_list_for_current_window = []
        with open(filename, 'r') as input_file:
            for line in input_file:
                # Append to the output list for the current window
                # the incremental output returned by a function call.
                # The function is passed the current line from the
                # file as an argument and the function returns the
                # increment and the next state.
                if state is not None:
                    output_increment, state = func(
                        line, state, *args, **kwargs)
                else:
                    output_increment = func(line, *args, **kwargs)
                output_list_for_current_window.append(output_increment)
                num_lines_read_in_current_window += 1
                if num_lines_read_in_current_window >= window_size:
                    # Extend out_stream with the entire window and
                    # then re-initialize the window, and finally sleep.
                    for v in output_list_for_current_window:
                         stream.append(v)
                    num_lines_read_in_current_window = 0
                    output_list_for_current_window = []
                    time.sleep(time_interval)
                    num_steps_taken += 1
                # The thread is ready because it has put at least one value
                # on the scheduler's input queue.
                ready.set()
                if num_steps is not None and num_steps_taken >= num_steps:
                    break
        return
 
    #------------------------------------------------------------------------
    # End of def thread_target(...)
    #------------------------------------------------------------------------

    return (
        threading.Thread(
            target=thread_target,
            args=(func, stream_name, filename, time_interval, num_steps,
                  window_size, state, ready, args, kwargs)),
        ready)


def source_list(
        in_list, stream, time_interval=0, num_steps=None, window_size=1,
        name='source_list'):
    """
    Puts elements of the list on to stream.

    """
    def read_list_func(state, in_list):
        return in_list[state], state+1
    return source_function(
        read_list_func, stream, time_interval, num_steps,
        window_size, state=0, name=name, in_list=in_list)
