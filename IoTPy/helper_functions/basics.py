"""
This module has the basic decorators of IoTPy

"""
import numpy as np

import sys
import os
sys.path.append(os.path.abspath("../helper_functions"))
sys.path.append(os.path.abspath("../core"))
sys.path.append(os.path.abspath("../agent_types"))

# agent and stream are in ../core
from agent import Agent
from stream import Stream, StreamArray, _multivalue
# recent_values is in ../helper_functions
from recent_values import recent_values
# op is in ../agent_types
from op import map_element, map_element_f
from op import filter_element, filter_element_f
from op import map_list, map_list_f
from op import timed_window
from op import map_window_f
from op import map_window, map_window_list
from helper_control import _no_value, _multivalue
from merge import zip_map, zip_map_f, merge_window_f, blend_f, blend
from merge import merge_window
from split import split_element_f, split_window_f, split_element
from split import split_element, split_window
from multi import multi_element_f, multi_window_f, multi_element, multi_window
from sink import sink_element, sink_window
from helper_control import _no_value
from run import run

#------------------------------------------------------------
#       WRAPPERS FOR DECORATORS
#------------------------------------------------------------

#-------------------------------------------------------------------
# map_element
#-------------------------------------------------------------------
def fmap_e(func):
    def wrapper(**kwargs):
        def g(s, **kwargs):
            return map_element_f(func, s, **kwargs)
        return g
    return wrapper()

def map_e(func):
    def wrapper(**kwargs):
        def g(in_stream, out_stream, **kwargs):
            map_element(func, in_stream, out_stream, **kwargs)
            return out_stream
        return g
    return wrapper()


#-------------------------------------------------------------------
# map_list
#-------------------------------------------------------------------
def fmap_l(func):
    def wrapper(**kwargs):
        def g(s, **kwargs):
            return map_list_f(func, s, **kwargs)
        return g
    return wrapper()

def map_l(func):
    def wrapper(**kwargs):
        def g(in_stream, out_stream, **kwargs):
            map_list(func, in_stream, out_stream, **kwargs)
            return out_stream
        return g
    return wrapper()

#-------------------------------------------------------------------
# map_window
#-------------------------------------------------------------------
def fmap_w(func):
    def wrapper(**kwargs):
        def g(in_stream, window_size, step_size, **kwargs):
            return map_window_f(func, in_stream, window_size, step_size, **kwargs)
        return g
    return wrapper()

def map_w(func):
    def wrapper(**kwargs):
        def g(in_stream, out_stream, window_size, step_size, **kwargs):
            return map_window(func, in_stream, out_stream,
                                window_size, step_size, **kwargs)
        return g
    return wrapper()

#-------------------------------------------------------------------
# map_window_list
#-------------------------------------------------------------------
def map_wl(func):
    def wrapper(**kwargs):
        def g(in_stream, out_stream, window_size, step_size, **kwargs):
            return map_window_list(func, in_stream, out_stream,
                                   window_size, step_size, **kwargs)
        return g
    return wrapper()

def fmap_wl(func):
    def wrapper(**kwargs):
        def g(in_stream, window_size, step_size, **kwargs):
            return map_window_list_f(func, in_stream, window_size, step_size, **kwargs)
        return g
    return wrapper()

#-------------------------------------------------------------------
# merge_element: same as zip_map
#-------------------------------------------------------------------
def fmerge_e(func):
    def wrapper(**kwargs):
        def g(in_streams, **kwargs):
            return zip_map_f(func, in_streams, **kwargs)
        return g
    return wrapper()

def merge_e(func):
    def wrapper(**kwargs):
        def g(in_streams, out_stream, **kwargs):
            return zip_map(func, in_streams, out_stream, **kwargs)
        return g
    return wrapper()

#-------------------------------------------------------------------
# merge_asynch
#-------------------------------------------------------------------

def merge_asynch(func):
    def wrapper(**kwargs):
        def g(in_streams, out_stream, **kwargs):
            return blend(func, in_streams, out_stream, **kwargs)
        return g
    return wrapper()

#-------------------------------------------------------------------
# merge_2e merge 2 elements. 
#-------------------------------------------------------------------
def fmerge_2e(func):
    def wrapper(**kwargs):
        def g(x, y, state=None, **kwargs):
            in_streams = [x, y]
            if state is None:
                def h_fmerge_2e(pair, **kwargs):
                    return func(pair[0], pair[1], **kwargs)
                return zip_map_f(h_fmerge_2e, in_streams, **kwargs)
            else:
                def h_fmerge_2e(pair, state, **kwargs):
                    return func(pair[0], pair[1], state, **kwargs)
                return zip_map_f(h_fmerge_2e, in_streams, state, **kwargs)
        return g
    return wrapper()
        

def fmerge_w(func):
    def wrapper(**kwargs):
        def g(in_streams, window_size, step_size, state=None, **kwargs):
            if state is None:
                return merge_window_f(
                    func, in_streams, window_size, step_size, **kwargs)
            else:
                return merge_window_f(
                    func, in_streams, window_size, step_size, state, **kwargs)
        return g
    return wrapper()

#-------------------------------------------------------------------
# merge_window
#-------------------------------------------------------------------
def merge_w(func):
    def wrapper(**kwargs):
        def g(in_streams, out_stream, window_size, step_size, state=None, **kwargs):
            if state is None:
                return merge_window(
                    func, in_streams, out_stream, window_size, step_size, **kwargs)
            else:
                return merge_window(
                    func, in_streams, out_stream, window_size, step_size, state, **kwargs)
        return g
    return wrapper()

def fmerge_2w(func):
    def wrapper(**kwargs):
        def g(in_stream_0, in_stream_1, window_size, step_size, state=None, **kwargs):
            in_streams = [in_stream_0, in_stream_1]
            if state is None:
                def h(v, **kwargs):
                    return func(v[0], v[1], **kwargs)
                return merge_window_f(
                    h, in_streams, window_size, step_size, **kwargs)
            else:
                def h(v, state, **kwargs):
                    return func(v[0], v[1], state, **kwargs)
                return merge_window_f(
                    h, in_streams, window_size, step_size, state, **kwargs)
        return g
    return wrapper()

#-------------------------------------------------------------------
# split_element
#-------------------------------------------------------------------
def split_e(func):
    def wrapper(**kwargs):
        def g(in_stream, out_streams, state=None, **kwargs):
            if state is None:
                return split_element(func, in_stream, out_streams, **kwargs)
            else:
                return split_element(func, in_stream, out_streams, state, **kwargs)
        return g
    return wrapper()


def fsplit_2e(func):
    def wrapper(**kwargs):
        def g(v, **kwargs):
            num_out_streams=2
            return split_element_f(func, v, num_out_streams, **kwargs)
        return g
    return wrapper()

#-------------------------------------------------------------------
# split_window
#-------------------------------------------------------------------
def split_w(func):
    def wrapper(**kwargs):
        def g(in_streams,  out_streams, window_size, step_size, state=None, **kwargs):
            if state is None:
                return split_window(
                    func, in_streams, out_streams,
                    window_size, step_size, **kwargs)
            else:
                return split_window(
                    func, in_streams, out_streams,
                    window_size, step_size, state, **kwargs)
                
        return g
    return wrapper()

#-------------------------------------------------------------------
# split_window into 2 streams
#-------------------------------------------------------------------
def fsplit_2w(func):
    def wrapper(**kwargs):
        def g(in_streams, window_size, step_size, **kwargs):
            num_out_streams = 2
            return split_window_f(
                func, in_streams, num_out_streams,
                window_size, step_size, **kwargs)
        return g
    return wrapper()

#-------------------------------------------------------------------
# multi_element
#-------------------------------------------------------------------
def multi_e(func):
    def wrapper(**kwargs):
        def g_multi_e(in_streams, out_streams, state=None, **kwargs):
            if state is None:
                return multi_element(func, in_streams, out_streams, **kwargs)
            else:
                return multi_element(func, in_streams, out_streams, state, **kwargs)
        return g_multi_e
    return wrapper()

#-------------------------------------------------------------------
# multi_window
#-------------------------------------------------------------------
def multi_w(func):
    def wrapper(**kwargs):
        def g_multi_w(
                in_streams, out_streams, 
                window_size, step_size, state=None, **kwargs):
            if state is None:
                return multi_window(
                    func, in_streams, out_streams,
                    window_size, step_size, **kwargs)
            else:
                return multi_window(
                    func, in_streams, out_streams,
                    window_size, step_size, state, **kwargs)
        return g_multi_w
    return wrapper()

#-------------------------------------------------------------------
# sink_element
#-------------------------------------------------------------------
def sink_e(func):
    def wrapper(**kwargs):
        def g(in_stream, **kwargs):
            sink_element(func, in_stream, **kwargs)
        return g
    return wrapper()


#-------------------------------------------------------------------
# sink_window
#-------------------------------------------------------------------
def sink_w(func):
    def wrapper(**kwargs):
        def g(in_stream, window_size, step_size, **kwargs):
            sink_window(func, in_stream,  window_size, step_size, **kwargs)
        return g
    return wrapper()

#------------------------------------------------------------
#       USEFUL FUNCTIONS OTHER THAN WRAPPERS
#------------------------------------------------------------

def prepend(lst, in_stream, out_stream):
    out_stream.extend(lst)
    map_element(lambda v: v, in_stream, out_stream)

def fprepend(lst, in_stream):
    out_stream = Stream()
    out_stream.extend(lst)
    map_element(lambda v: v, in_stream, out_stream)
    return out_stream

@fmap_e
def exponential_smoothing(v, current_state, alpha):
   next_state = alpha*current_state + (1 - alpha)*v
   next_output = next_state
   return next_output, next_state

@fmap_e
def filter_min(x, min_value):
    return x if abs(x) >= min_value else _no_value

def f_mul(in_stream, arg):
    @fmap_e
    def times(v, arg): return v*arg
    return times(in_stream, arg=arg)

def r_mul(in_stream, out_stream, arg):
    @map_e
    def times(v, arg): return v*arg
    return times(in_stream, out_stream, arg=arg)

def f_add(in_stream, arg):
    @fmap_e
    def plus(v, arg): return v+arg
    return plus(in_stream, arg=arg)

def r_add(in_stream, out_stream, arg):
    @map_e
    def plus(v, arg): return v+arg
    return plus(in_stream, out_stream, arg=arg)

def f_sub(in_stream, arg):
    @fmap_e
    def f(v, arg): return v - arg
    return f(in_stream, arg=arg)

def r_sub(in_stream, out_stream, arg):
    @map_e
    def f(v, arg): return v - arg
    return f(in_stream, out_stream, arg=arg)

def minimum(in_stream, arg):
    @fmap_e
    def mini(x, arg): return min(x, arg)
    return mini(in_stream, arg=arg)

def maximum(in_stream, arg):
    @fmap_e
    def maxi(x, arg): return max(x, arg)
    return maxi(in_stream, arg=arg)

def clip(in_stream, arg):
    @fmap_e
    def f(x, arg):
        if x < -arg: return -arg
        elif x > arg: return arg
        else: return x
    return f(in_stream, arg=arg)

def sieve(in_stream, primes):
    out_stream = Stream()
    @map_e
    def f(v, state, primes):
        my_prime, last = state
        output = _no_value
        if my_prime == 0:
            my_prime = v
            primes.append(v)
        else:
            if v % my_prime != 0:
                output = v
                if last:
                    last = False
                    sieve(out_stream, primes)
        return output, (my_prime, last)
    f(in_stream, out_stream, state=(0, True), primes=primes)

def make_echo(spoken, D, A):
    echo = Stream(name='echo', initial_value=[0]*D)
    heard = spoken + echo
    r_mul(in_stream=heard, out_stream=echo, arg=A)
    return heard

@sink_e
def print_stream(v, stream_name=None):
    if stream_name is not None:
        print(stream_name, ' : ', v)
    else:
        print(v)


        
#------------------------------------------------------------
#------------------------------------------------------------
#       TESTS
#------------------------------------------------------------
#------------------------------------------------------------

def test_f_mul():
    x = Stream()
    y = f_mul(x, 2)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [0, 2, 4, 6, 8]
    
def test_r_mul():
    x = Stream()
    y = Stream()
    a = r_mul(x, y, 2)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [0, 2, 4, 6, 8]

def test_f_add():
    x = Stream()
    K = 5
    y = f_add(x, K)
    x.extend(list(range(3)))
    run()
    assert recent_values(y) == [5, 6, 7]

def test_r_add():
    x = Stream()
    y = Stream()
    z = 5
    r_add(x, y, z)
    x.extend(list(range(3)))
    run()
    assert recent_values(y) == [5, 6, 7]

def test_f_sub():
    x = Stream()
    y = f_sub(x, 2)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [-2, -1, 0, 1, 2]

def test_r_sub():
    x = Stream()
    y = Stream()
    r_sub(x, y, 2)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [-2, -1, 0, 1, 2]


def test_minimum():
    x = Stream()
    y = minimum(x, 2)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [0, 1, 2, 2, 2]

def test_maximum():
    x = Stream()
    y = maximum(x, 2)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [2, 2, 2, 3, 4]

def test_clip():
    x = Stream()
    y = clip(x, 2)
    x.extend([0, 1, 2, 3, -1, -2, -3])
    run()
    assert recent_values(y) == [0, 1, 2, 2, -1, -2, -2]

def test_exponential_smoothing():
    x = Stream()
    y = exponential_smoothing(x, state=0, alpha=0.5)
    x.extend([64, 32, 16, 8, 4, 2, 1])
    run()
    assert recent_values(y) == [
        32.0, 32.0, 24.0, 16.0, 10.0, 6.0, 3.5]
    
def test_plus_operator():
    x = Stream()
    y = Stream()
    z = x + y
    
    x.extend(list(range(3)))
    y.extend(list(range(100, 105)))
    run()
    assert recent_values(z) == [
        100, 102, 104]

    x.extend(list(range(3, 7)))
    run()
    assert recent_values(z) == [
        100, 102, 104, 106, 108]

    run()
    assert recent_values(z) == [
        100, 102, 104, 106, 108]

def test_plus_operator_with_arrays_1():
    x = StreamArray(dtype=int)
    y = StreamArray(dtype=int)
    z = x + y
    
    x.extend(np.arange(3))
    y.extend(np.arange(100, 105))
    run()
    assert isinstance(recent_values(z), np.ndarray)
    assert np.array_equal(recent_values(z), np.array([100, 102, 104]))

    x.extend(np.arange(3, 7))
    run()
    assert np.array_equal(recent_values(z), np.array([
        100, 102, 104, 106, 108]))

    run()
    assert np.array_equal(recent_values(z), np.array([
        100, 102, 104, 106, 108]))

def test_plus_operator_with_arrays():
    x = StreamArray(dimension=2, dtype=int)
    y = StreamArray(dimension=2, dtype=int)
    z = x + y
    A = np.arange(6).reshape((3, 2))
    B = np.arange(100, 110).reshape((5, 2))
    x.extend(A)
    y.extend(B)
    run()
    assert isinstance(z, StreamArray)
    assert np.array_equal(recent_values(z), np.array([
        [100, 102], [104, 106], [108, 110]]))

    C = np.arange(6, 12).reshape((3, 2))
    x.extend(C)
    run()
    assert np.array_equal(recent_values(z), np.array([
        [100, 102], [104, 106], [108, 110],
        [112, 114], [116, 118]]))


def test_minus_operator_with_arrays():
    x = StreamArray(dtype=int)
    y = StreamArray(dtype=int)
    z = y - x
    
    x.extend(np.arange(3))
    y.extend(np.arange(100, 105, 2))
    run()
    assert np.array_equal(recent_values(z), np.array([
        100, 101, 102]))

def test_minus_operator_with_arrays_and_dimension():
    x = StreamArray(dimension=3, dtype=int)
    y = StreamArray(dimension=3, dtype=int)
    z = y - x
    A = np.array([[10, 20, 30], [40, 50, 60]])
    B= np.array([[100, 100, 100], [200, 200, 200], [300, 300, 00]])
    x.extend(A)
    y.extend(B)
    run()
    assert np.array_equal(recent_values(z), np.array([
        [ 90,  80,  70],
        [160, 150, 140]]))

def test_prepend():
    from run import run
    x = Stream()
    y = Stream()
    prepend(list(range(10)), x, y)
    z = fprepend(list(range(10)), x)
    x.extend(list(range(100, 105)))
    run()
    assert recent_values(x) == [
        100, 101, 102, 103, 104]
    assert recent_values(y) == [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        100, 101, 102, 103, 104]
    assert recent_values(z) == [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        100, 101, 102, 103, 104]
    

def test_filter_min():
    from run import run
    x = Stream()
    y = filter_min(x, min_value=0.5)
    x.extend([64, 32, 16, 8, 4, 2, 1, 0.5, 0.25, 0.125])
    run()
    assert recent_values(y) == [64, 32, 16, 8, 4, 2, 1, 0.5]


def test_sieve():
    x = Stream()
    primes = []
    sieve(x, primes)
    x.extend(list(range(2, 30)))
    Stream.scheduler.step()
    assert primes == [
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29]

def test_echo():
    spoken = Stream('spoken')
    heard = make_echo(spoken, D=1, A=0.5)
    spoken.extend([64, 32, 16, 8, 4, 2, 1, 0, 0, 0, 0])
    run()
    assert recent_values(heard) == [
        64.0, 64.0, 48.0, 32.0, 20.0, 12.0, 7.0, 3.5, 1.75, 0.875, 0.4375]

def test_print_stream():
    # Test print_stream which is a sink object (i.e. no output).
    s = Stream()
    print_stream(s)
    s.extend(list(range(2)))
    run()

def test_sink():
    # Test sink with state
    @sink_e
    def f(v, state, addend, output_list):
        output_list.append(v+state)
        state +=addend
        return state

    s = Stream()
    output_list = []
    f(s, state=0, addend=10, output_list=output_list)
    s.extend(list(range(5)))
    run()
    assert output_list == [0, 11, 22, 33, 44]


def test_source_file(filename):
    s = Stream('s')
    with open(filename, 'r') as input_file:
        for line in input_file:
            s.append(int(line))
            run()
    assert recent_values(s) == [1, 2, 3]

def test_delay():
    y = Stream(initial_value=[0]*5)
    x = Stream()
    @map_e
    def f(v): return 2*v
    f(x, y)
    x.extend(list(range(10)))
    run()
    assert recent_values(y) == [
        0, 0, 0, 0, 0, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

def test_map_with_state():
    x = Stream()
    y = Stream()
    @map_e
    def f(v, state): return v+ state, state+1
    f(x, y, state=0)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [0, 2, 4, 6, 8]

def test_map_window_with_state():
    x = Stream()
    y = Stream()
    @map_w
    def f(window, state): return sum(window)+state, state+1
    f(x, y, window_size=2, step_size=2, state=0)
    x.extend(list(range(10)))
    run()
    assert recent_values(y) ==  [1, 6, 11, 16, 21]

def test_map_with_keyword_arg():
    x = Stream()
    y = Stream()
    @map_e
    def f(v, k): return v+ k
    f(x, y, k=10)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [10, 11, 12, 13, 14]

def test_map_with_state_and_keyword_arg():
    x = Stream()
    y = Stream()
    @map_e
    def f(v, state, k): return v+k+state, state+1
    @fmap_e
    def g(v, state, k): return v+k+state, state+1
        
    f(x, y, state=0, k=10)
    z = g(x, state=0, k=10)
    x.extend(list(range(5)))
    run()
    assert recent_values(y) == [10, 12, 14, 16, 18]
    assert recent_values(z) == [10, 12, 14, 16, 18]

def test_fmap_with_stream_array():
    x = StreamArray(dimension=2, dtype=int)
    @fmap_e
    def g(v): return 2*v
    y = g(x)

    A = np.array([[1, 10], [2, 20], [3, 30]])
    x.extend(A)
    run()
    assert np.array_equal(
        recent_values(y),
        [[2, 20], [4, 40], [6, 60]])

    x.append(np.array([4, 40]))
    run()
    assert np.array_equal(
        recent_values(y),
        [[2, 20], [4, 40], [6, 60], [8, 80]])

def test_map_window_list_0():
    x = Stream()
    y = Stream()
    @map_wl
    def f(window): return window
    f(x, y, window_size=2, step_size=2)
    x.extend(list(range(10)))
    run()
    assert recent_values(y) ==  list(range(10))

def test_map_window_list_1():
    x = Stream()
    y = Stream()
    @map_wl
    def f(window): return [2*v for v in window]
    f(x, y, window_size=2, step_size=2)
    x.extend(list(range(10)))
    run()
    assert recent_values(y) ==  list(range(0, 20, 2))

def test_map_window_list_2():
    x = Stream()
    y = Stream()
    @map_wl
    def f(window, state):
        return [v+10*state for v in window], state+1
    f(x, y, window_size=2, step_size=2, state=0)
    x.extend(list(range(10)))
    run()
    assert recent_values(y) == [
        0, 1, 12, 13, 24, 25, 36, 37, 48, 49]

def test_map_window_list_3():
    x = Stream()
    y = Stream()
    @map_wl
    def f(window, state, K):
        return [v+10*state+K for v in window], state+1
    f(x, y, window_size=2, step_size=2, state=0, K=100)
    x.extend(list(range(10)))
    run()
    assert recent_values(y) == [
        100, 101, 112, 113, 124, 125, 136, 137, 148, 149]

def test_sink_1():
    x = Stream()
    y = Stream()
    @sink_w
    def f(v, out_stream):
        out_stream.append(sum(v)+10)
    f(x, window_size=2, step_size=1, out_stream=y)
    x.extend(list(range(5)))
    run()
    
def test_sink_2():
    x = Stream()
    y = Stream()
    @sink_e
    def f(v, out_stream):
        out_stream.append(v+100)
    f(x, out_stream=y)
    x.extend(list(range(5)))
    run()

def test_sink_3():
    x = StreamArray(dtype='int')
    y = StreamArray()
    @sink_w
    def f(window, y):
        y.append(window[-1] - np.mean(window))
    f(x, window_size=2, step_size=1, y=y)
    x.extend(np.arange(5))
    run()
    assert (np.array_equal
            (recent_values(y), np.array([0.5, 0.5, 0.5, 0.5]))) 

def test_map_list_with_arrays():
    from op import map_list
    x = StreamArray(dtype=int)
    y = StreamArray(dtype=int)
    def f(A): return 2*A

    map_list(f, x, y)
    x.extend(np.arange(5))
    run()
    assert np.array_equal(recent_values(y), 2*np.arange(5))

def test_merge_1():
    @merge_e
    def f(list_of_numbers):
        return sum(list_of_numbers)
    x = Stream('x')
    y = Stream('y')
    z = Stream('z')

    f([x,y], z)
    x.extend(list(range(5)))
    y.extend(list(range(10)))
    run()
    assert(recent_values(z) == [0, 2, 4, 6, 8])


    
#------------------------------------------------------------
#       RUN TESTS
#------------------------------------------------------------
    
if __name__ == '__main__':
    test_map_with_keyword_arg()
    test_sink_1()
    test_sink_2()
    test_sink_3()
    test_f_mul()
    test_r_mul()
    test_f_add()
    test_r_add()
    test_f_sub()
    test_r_sub()
    test_minimum()
    test_maximum()
    test_clip()
    test_sieve()
    test_plus_operator()
    test_exponential_smoothing()
    test_prepend()
    test_filter_min()
    test_echo()
    test_sink()
    test_source_file('test_source_file_name.txt')
    test_delay()
    test_map_with_state()
    test_map_window_with_state()
    test_map_with_state_and_keyword_arg()
    test_map_list_with_arrays()
    test_plus_operator_with_arrays()
    test_minus_operator_with_arrays()
    test_minus_operator_with_arrays_and_dimension()
    test_plus_operator_with_arrays_1()
    test_fmap_with_stream_array()
    test_map_window_list_0()
    test_map_window_list_1()
    test_map_window_list_2()
    test_map_window_list_3()
    test_merge_1()

    
    
    
    
    

