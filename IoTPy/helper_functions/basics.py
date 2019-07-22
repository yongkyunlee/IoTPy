"""
This module tests element_agent.py

"""
import numpy as np

import sys
import os
sys.path.append(os.path.abspath("../helper_functions"))
sys.path.append(os.path.abspath("../core"))
sys.path.append(os.path.abspath("../agent_types"))

# agent and stream are in ../core
from agent import Agent
from stream import Stream, StreamArray, _no_value, _multivalue
# recent_values is in ../helper_functions
from recent_values import recent_values
# op is in ../agent_types
from op import map_element, map_element_f
from op import filter_element, filter_element_f
from op import map_list, map_list_f
from op import timed_window
from op import map_window_f
from op import map_window
from helper_control import _no_value, _multivalue
from merge import zip_map_f, merge_window_f, blend_f
from split import split_element_f, split_window_f
from multi import multi_element_f, multi_window_f


def map_e(func):
    def wrapper(**kwargs):
        def g(s, **kwargs):
            return map_element_f(func, s, **kwargs)
        return g
    return wrapper()

def map_w(func):
    def wrapper(**kwargs):
        def g(s, window_size, step_size, **kwargs):
            return map_window_f(func, s, window_size, step_size, **kwargs)
        return g
    return wrapper()

def merge_e(func):
    def wrapper(**kwargs):
        def g(lst, **kwargs):
            return zip_map_f(func, lst, **kwargs)
        return g
    return wrapper()


def merge_fair(func):
    def wrapper(**kwargs):
        def g(lst, **kwargs):
            return blend_f(func, lst, **kwargs)
        return g
    return wrapper()


def merge_2_e(func):
    def wrapper(**kwargs):
        def g(x, y, state=None, **kwargs):
            in_streams = [x, y]
            if state is None:
                def h_merge_2_e(pair, **kwargs):
                    return func(pair[0], pair[1], **kwargs)
                return zip_map_f(h_merge_2_e, in_streams, **kwargs)
            else:
                def h_merge_2_e(pair, state, **kwargs):
                    return func(pair[0], pair[1], state, **kwargs)
                return zip_map_f(h_merge_2_e, in_streams, state, **kwargs)
        return g
    return wrapper()
        

def merge_w(func):
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

def merge_2_w(func):
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


def split_e(func):
    def wrapper(**kwargs):
        def g(in_stream, num_out_streams, state=None, **kwargs):
            if state is None:
                return split_element_f(func, in_stream, num_out_streams, **kwargs)
            else:
                return split_element_f(func, in_stream, num_out_streams, state, **kwargs)
        return g
    return wrapper()


def split_2_e(func):
    def wrapper(**kwargs):
        def g(v, **kwargs):
            num_out_streams=2
            return split_element_f(func, v, num_out_streams, **kwargs)
        return g
    return wrapper()


def split_w(func):
    def wrapper(**kwargs):
        def g(in_streams,  num_out_streams, window_size, step_size, state=None, **kwargs):
            if state is None:
                return split_window_f(
                    func, in_streams, num_out_streams,
                    window_size, step_size, **kwargs)
            else:
                return split_window_f(
                    func, in_streams, num_out_streams,
                    window_size, step_size, state, **kwargs)
                
        return g
    return wrapper()


def split_2_w(func):
    def wrapper(**kwargs):
        def g(in_streams, window_size, step_size, **kwargs):
            num_out_streams = 2
            return split_window_f(
                func, in_streams, num_out_streams,
                window_size, step_size, **kwargs)
        return g
    return wrapper()

def multi_e(func):
    def wrapper(**kwargs):
        def g_multi_e(in_streams, num_out_streams, state=None, **kwargs):
            if state is None:
                return multi_element_f(func, in_streams, num_out_streams, **kwargs)
            else:
                return multi_element_f(func, in_streams, num_out_streams, state, **kwargs)
        return g_multi_e
    return wrapper()


def multi_w(func):
    def wrapper(**kwargs):
        def g_multi_w(
                in_streams, num_out_streams, 
                window_size, step_size, state=None, **kwargs):
            if state is None:
                return multi_window_f(
                    func, in_streams, num_out_streams,
                    window_size, step_size, **kwargs)
            else:
                return multi_window_f(
                    func, in_streams, num_out_streams,
                    window_size, step_size, state, **kwargs)
        return g_multi_w
    return wrapper()

#------------------------------------------------------------
#       TEST
#------------------------------------------------------------
def test_try():
    @map_e
    def h(v): return 2*v

    s = Stream()
    t = h(s)
    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(t) == [0, 2, 4, 6, 8]

    @map_w
    def h(v): return sum(v)

    s = Stream()
    t = h(s, window_size=2, step_size=2)
    s.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(t) == [1, 5, 9, 13, 17]

    @map_w
    def h(v, addend): return sum(v) + addend

    s = Stream()
    t = h(s, window_size=2, step_size=2, addend=10)
    s.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(t) == [11, 15, 19, 23, 27]

    @map_w
    def h(v, state, addend):
        next_state = state + 1
        return sum(v) + state + addend, next_state

    s = Stream()
    t = h(s, window_size=2, step_size=2, state= 0, addend=10)
    s.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(t) == [11, 16, 21, 26, 31]

    @map_e
    def h(v):
        return _multivalue((v, v+10)) if v%2 else _no_value

    s = Stream()
    t = h(s)
    s.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(t) == [1, 11, 3, 13, 5, 15, 7, 17, 9, 19]


    class add(object):
        def __init__(self, addend):
            self.addend = addend
        def func(self, v):
            return _multivalue((v, v+self.addend)) if v%2 else _no_value

    add_object = add(10)
    @map_e
    def h(v):
        return add_object.func(v)

    s = Stream()
    t = h(s)
    s.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(t) == [1, 11, 3, 13, 5, 15, 7, 17, 9, 19]

    class addition(object):
       def __init__(self, addend):
          self.addend = addend
          self.total = 0
       def add(self, v):
          self.total += self.addend
          return v + self.addend

    add_10 = addition(addend=10)
    @map_e
    def f(v): return add_10.add(v)

    x = Stream('x')
    y = f(x)
    x.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(y) == [
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

    @map_e
    def f(v, addend): return v + addend

    x = Stream('x')
    y = f(x, addend=10)
    x.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(y) == [
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

    @map_e
    def f(v, state, addend):
        next_state = state + 1
        return v + addend + state, next_state

    x = Stream('x')
    y = f(x, state=0, addend=10)
    x.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(y) == [
        10, 12, 14, 16, 18, 20, 22, 24, 26, 28]
    

    @map_e
    def f(v, state):
        next_state = state + 1
        return v + state, next_state

    x = Stream('x')
    y = f(x, state=0)
    x.extend(range(10))
    Stream.scheduler.step()
    assert recent_values(y) == [
        0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    @merge_e
    def hh(l):
        return sum(l)

    x = Stream('X')
    y = Stream('Y')
    t = hh([x, y])
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        100, 102, 104, 106, 108,
        110, 112, 114, 116, 118]

    @merge_e
    def hh(l, addend):
        return sum(l) + addend

    x = Stream('X')
    y = Stream('Y')
    t = hh([x, y], addend=1000)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        1100, 1102, 1104, 1106, 1108,
        1110, 1112, 1114, 1116, 1118]

    

    @merge_e
    def hh(l, state, addend):
        next_state = state + 1
        return sum(l) + addend + state, next_state

    x = Stream('X')
    y = Stream('Y')
    t = hh([x, y], state=0, addend=1000)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        1100, 1103, 1106, 1109, 1112,
        1115, 1118, 1121, 1124, 1127]

    @merge_e
    def hh(l, state):
        next_state = state + 1
        return sum(l) + state, next_state

    x = Stream('X')
    y = Stream('Y')
    t = hh([x, y], state=0)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        100, 103, 106, 109, 112, 115, 118, 121, 124, 127]


    @merge_fair
    def h(v):
        return 2*v

    x = Stream('X')
    y = Stream('Y')
    t = h([x, y])
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    #print recent_values(t)


    @merge_fair
    def h(v, state):
        next_state = state+1
        return 2*v+state, next_state

    x = Stream('X')
    y = Stream('Y')
    t = h([x, y], state=0)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    #print recent_values(t)


    @merge_fair
    def h(v, addend):
        return 2*v + addend

    x = Stream('X')
    y = Stream('Y')
    t = h([x, y], addend=1000)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    #print recent_values(t)
    
    @merge_2_e
    def h(x,y):
        return x+2*y

    x = Stream()
    y = Stream()
    t = h(x, y)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        200, 203, 206, 209, 212, 215,
        218, 221, 224, 227]
    
    @merge_2_e
    def h(x, y, addend):
        return x+2*y + addend

    x = Stream()
    y = Stream()
    t = h(x, y, addend=1000)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        1200, 1203, 1206, 1209, 1212, 1215,
        1218, 1221, 1224, 1227]

    
    
    @merge_2_e
    def h(x, y, state, addend):
        next_state = state + 1
        return x+2*y + addend + state, next_state

    x = Stream()
    y = Stream()
    t = h(x, y, state= 0, addend=1000)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        1200, 1204, 1208, 1212, 1216,
        1220, 1224, 1228, 1232, 1236]

    
    
    @merge_2_e
    def h(x, y, state):
        next_state = state + 1
        return x+2*y + state, next_state

    x = Stream()
    y = Stream()
    t = h(x, y, state= 0)
    x.extend(range(10))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        200, 204, 208, 212, 216,
        220, 224, 228, 232, 236]


    @merge_w
    def h(list_of_windows):
        window_0, window_1 = list_of_windows
        return sum(window_0) + 2*sum(window_1)
    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = h(in_streams, window_size=2, step_size=2)

    x.extend(range(20))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        403, 415, 427, 439, 451, 463,
        475, 487, 499, 511]


    @merge_w
    def h(list_of_windows, addend):
        window_0, window_1 = list_of_windows
        return sum(window_0) + 2*sum(window_1) + addend
    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = h(in_streams, window_size=2, step_size=2, addend=1000)

    x.extend(range(20))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        1403, 1415, 1427, 1439, 1451, 1463,
        1475, 1487, 1499, 1511]


    @merge_w
    def h(list_of_windows, state, addend):
        next_state = state + 1
        window_0, window_1 = list_of_windows
        return sum(window_0) + 2*sum(window_1) + addend + state, next_state
    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = h(in_streams, window_size=2, step_size=2, state=0, addend=1000)

    x.extend(range(20))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        1403, 1416, 1429, 1442, 1455,
        1468, 1481, 1494, 1507, 1520]


    @merge_w
    def h(list_of_windows, state):
        next_state = state + 1
        window_0, window_1 = list_of_windows
        return sum(window_0) + 2*sum(window_1) + state, next_state
    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = h(in_streams, window_size=2, step_size=2, state=0)

    x.extend(range(20))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        403, 416, 429, 442, 455, 468, 481, 494, 507, 520]

    @merge_2_w
    def h(window_0, window_1):
        return sum(window_0) + 2*sum(window_1)
    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = h(x, y, window_size=2, step_size=2)

    x.extend(range(20))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        403, 415, 427, 439, 451, 463,
        475, 487, 499, 511]


    @merge_2_w
    def hhhh(window_0, window_1, addend):
        return sum(window_0) + 2*sum(window_1) + addend
    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = hhhh(x, y, window_size=2, step_size=2, addend=1000)

    x.extend(range(20))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        1403, 1415, 1427, 1439, 1451, 1463,
        1475, 1487, 1499, 1511]


    @merge_2_w
    def hhhh(window_0, window_1, state, addend):
        next_state = state + 1
        return sum(window_0) + 2*sum(window_1) + addend + state, next_state
    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = hhhh(x, y, window_size=2, step_size=2, state=0, addend=1000)

    x.extend(range(20))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        1403, 1416, 1429, 1442, 1455,
        1468, 1481, 1494, 1507, 1520]


    @merge_2_w
    def hhhh(window_0, window_1, state):
        next_state = state + 1
        return sum(window_0) + 2*sum(window_1) + state, next_state
    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = hhhh(x, y, window_size=2, step_size=2, state=0)

    x.extend(range(20))
    y.extend(range(100, 120))
    
    Stream.scheduler.step()
    assert recent_values(t) == [
        403, 416, 429, 442, 455, 468,
        481, 494, 507, 520]

    @split_e
    def h(v):
        return [v, v+1000]
    s = Stream()
    u, v = h(s, num_out_streams=2)

    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(u) == [0, 1, 2, 3, 4]
    assert recent_values(v) == [1000, 1001, 1002, 1003, 1004]

    @split_e
    def h(v, addend):
        return [v+addend, v+1000+addend]
    s = Stream()
    u, v = h(s, num_out_streams=2, addend=10)

    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(u) == [10, 11, 12, 13, 14]
    assert recent_values(v) == [1010, 1011, 1012, 1013, 1014]

    
    @split_e
    def h(v, state, addend):
        next_state = state + 1
        return [v+addend+state, v+1000+addend+state], next_state
    s = Stream()
    u, v = h(s, state=0, num_out_streams=2, addend=10)

    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(u) == [10, 12, 14, 16, 18]
    assert recent_values(v) == [1010, 1012, 1014, 1016, 1018]

    
    @split_e
    def h(v, state):
        next_state = state + 1
        return [v+state, v+1000+state], next_state
    s = Stream()
    u, v = h(s, state=0, num_out_streams=2)

    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(u) == [0, 2, 4, 6, 8]
    assert recent_values(v) == [1000, 1002, 1004, 1006, 1008]


    @split_2_e
    def h_split_2_e(v):
        return [v, v+1000]
    s = Stream()
    u, v = h_split_2_e(s)

    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(u) == [0, 1, 2, 3, 4]
    assert recent_values(v) == [1000, 1001, 1002, 1003, 1004]

    @split_2_e
    def hk(v, addend):
        return [v+addend, v+1000+addend]
    s = Stream()
    u, v = hk(s, addend=10)

    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(u) == [10, 11, 12, 13, 14]
    assert recent_values(v) == [1010, 1011, 1012, 1013, 1014]


    @split_2_e
    def hk(v, state, addend):
        next_state = state + 1
        return [v+addend+state, v+1000+addend+state], next_state
    s = Stream()
    u, v = hk(s, state=0, addend=10)

    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(u) == [10, 12, 14, 16, 18]
    assert recent_values(v) == [1010, 1012, 1014, 1016, 1018]


    @split_2_e
    def hk(v, state):
        next_state = state + 1
        return [v+state, v+1000+state], next_state
    s = Stream()
    u, v = hk(s, state=0)

    s.extend(range(5))
    Stream.scheduler.step()
    assert recent_values(u) == [0, 2, 4, 6, 8]
    assert recent_values(v) == [1000, 1002, 1004, 1006, 1008]


    @split_w
    def h(window):
        return [sum(window), max(window)]
    s = Stream()
    u, v = h(s, num_out_streams=2, window_size=3, step_size=2)

    s.extend(range(12))
    Stream.scheduler.step()
    assert recent_values(u) == [3, 9, 15, 21, 27]
    assert recent_values(v) == [2, 4, 6, 8, 10]


    @split_w
    def h(window, addend):
        return [sum(window)+addend, max(window)+addend]
    s = Stream()
    u, v = h(s, num_out_streams=2, window_size=3, step_size=2, addend=1000)

    s.extend(range(12))
    Stream.scheduler.step()
    assert recent_values(u) == [1003, 1009, 1015, 1021, 1027]
    assert recent_values(v) == [1002, 1004, 1006, 1008, 1010]


    @split_w
    def h(window, state, addend):
        next_state = state + 1
        return [sum(window)+addend+state, max(window)+addend+state], next_state
    s = Stream()
    u, v = h(s, num_out_streams=2, window_size=3, step_size=2, state=0, addend=1000)

    s.extend(range(12))
    Stream.scheduler.step()
    assert recent_values(u) == [1003, 1010, 1017, 1024, 1031]
    assert recent_values(v) == [1002, 1005, 1008, 1011, 1014]


    @split_w
    def h(window, state):
        next_state = state + 1
        return [sum(window)+state, max(window)+state], next_state
    s = Stream()
    u, v = h(s, num_out_streams=2, window_size=3, step_size=2, state=0)

    s.extend(range(12))
    Stream.scheduler.step()
    assert recent_values(u) == [3, 10, 17, 24, 31]
    assert recent_values(v) == [2, 5, 8, 11, 14]



    @split_2_w
    def h(window):
        return [sum(window), max(window)]
    s = Stream()
    u, v = h(s, window_size=3, step_size=2)

    s.extend(range(12))
    Stream.scheduler.step()
    assert recent_values(u) == [3, 9, 15, 21, 27]
    assert recent_values(v) == [2, 4, 6, 8, 10]


    @split_2_w
    def h(window, addend):
        return [sum(window)+addend, max(window)+addend*2]
    s = Stream()
    u, v = h(s, window_size=3, step_size=2, addend=1000)

    s.extend(range(12))
    Stream.scheduler.step()
    assert recent_values(u) == [1003, 1009, 1015, 1021, 1027]
    assert recent_values(v) == [2002, 2004, 2006, 2008, 2010]


    @split_2_w
    def h(window, state, addend):
        next_state = state + 1
        return [sum(window)+addend+state, max(window)+addend*2-state], next_state
    s = Stream()
    u, v = h(s, window_size=3, step_size=2, state=0, addend=1000)

    s.extend(range(12))
    Stream.scheduler.step()
    assert recent_values(u) == [1003, 1010, 1017, 1024, 1031]
    assert recent_values(v) == [2002, 2003, 2004, 2005, 2006]


    @split_2_w
    def h(window, state):
        next_state = state + 1
        return [sum(window)+state, max(window)-state], next_state
    s = Stream()
    u, v = h(s, window_size=3, step_size=2, state=0)

    s.extend(range(12))
    Stream.scheduler.step()
    assert recent_values(u) == [3, 10, 17, 24, 31]
    assert recent_values(v) == [2, 3, 4, 5, 6]


    @multi_e
    def f(a_list):
        # Number of values returned in num_out_streams
        # which is 2.
        return max(a_list), sum(a_list)

    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = f(in_streams, num_out_streams=2)
    u, v = t

    x.extend(range(10))
    y.extend(range(100, 110))
    Stream.scheduler.step()
    assert recent_values(u) == [
        100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
    assert recent_values(v) == [
        100, 102, 104, 106, 108, 110, 112, 114, 116, 118]


    @multi_e
    def f(a_list, addend):
        # Number of values returned in num_out_streams
        # which is 2.
        return max(a_list)+addend, sum(a_list)+addend

    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = f(in_streams, num_out_streams=2, addend=1000)
    u, v = t

    x.extend(range(10))
    y.extend(range(100, 110))
    Stream.scheduler.step()
    assert recent_values(u) == [
        1100, 1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109]
    assert recent_values(v) == [
        1100, 1102, 1104, 1106, 1108, 1110, 1112, 1114, 1116, 1118]


    @multi_e
    def f(a_list, state, addend):
        # Number of values returned in num_out_streams
        # which is 2.
        next_state = state + 1
        return [max(a_list)+addend+state, sum(a_list)+addend+state], next_state

    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = f(in_streams, num_out_streams=2, state=0, addend=1000)
    u, v = t

    x.extend(range(10))
    y.extend(range(100, 110))
    Stream.scheduler.step()
    assert recent_values(u) == [
        1100, 1102, 1104, 1106, 1108, 1110, 1112, 1114, 1116, 1118]
    assert recent_values(v) == [
        1100, 1103, 1106, 1109, 1112, 1115, 1118, 1121, 1124, 1127]


    @multi_e
    def f(a_list, state):
        # Number of values returned in num_out_streams
        # which is 2.
        next_state = state + 1
        return [max(a_list)+state, sum(a_list)+state], next_state

    x = Stream()
    y = Stream()
    in_streams = [x, y]
    t = f(in_streams, num_out_streams=2, state=0)
    u, v = t

    x.extend(range(10))
    y.extend(range(100, 110))
    Stream.scheduler.step()
    assert recent_values(u) == [
        100, 102, 104, 106, 108, 110, 112, 114, 116, 118]
    assert recent_values(v) == [
        100, 103, 106, 109, 112, 115, 118, 121, 124, 127]


    @multi_w
    def f_multi_window(pair_of_windows):
        window_0, window_1 = pair_of_windows
        output_0 = sum(window_0) + sum(window_1)
        output_1 = max(window_0) + max(window_1)
        return (output_0, output_1)
    w = Stream('w')
    x = Stream('x')
    y, z = f_multi_window(
        in_streams=[w,x], num_out_streams=2,
        window_size=2, step_size=2)
    
    w.extend(range(10))
    x.extend(range(100, 120, 2))
    
    Stream.scheduler.step()
    assert recent_values(y) == [203, 215, 227, 239, 251]
    assert recent_values(z) == [103, 109, 115, 121, 127]


    @multi_w
    def f_multi_window(pair_of_windows, addend):
        window_0, window_1 = pair_of_windows
        output_0 = sum(window_0) + sum(window_1) + addend
        output_1 = max(window_0) + max(window_1) + addend
        return (output_0, output_1)
    w = Stream('w')
    x = Stream('x')
    y, z = f_multi_window(
        in_streams=[w,x], num_out_streams=2,
        window_size=2, step_size=2, addend=1000)
    
    w.extend(range(10))
    x.extend(range(100, 120, 2))
    
    Stream.scheduler.step()
    assert recent_values(y) == [1203, 1215, 1227, 1239, 1251]
    assert recent_values(z) == [1103, 1109, 1115, 1121, 1127]
    

    @multi_w
    def f_multi_window(pair_of_windows, state, addend):
        window_0, window_1 = pair_of_windows
        output_0 = sum(window_0) + sum(window_1) + addend + state
        output_1 = max(window_0) + max(window_1) + addend + state
        next_state = state + 1
        return (output_0, output_1), next_state
    w = Stream('w')
    x = Stream('x')
    y, z = f_multi_window(
        in_streams=[w,x], num_out_streams=2,
        window_size=2, step_size=2, state=0, addend=1000)
    
    w.extend(range(10))
    x.extend(range(100, 120, 2))
    
    Stream.scheduler.step()
    assert recent_values(y) == [1203, 1216, 1229, 1242, 1255]
    assert recent_values(z) == [1103, 1110, 1117, 1124, 1131]
    

    @multi_w
    def f_multi_window(pair_of_windows, state):
        window_0, window_1 = pair_of_windows
        output_0 = sum(window_0) + sum(window_1) + state
        output_1 = max(window_0) + max(window_1) + state
        next_state = state + 1
        return (output_0, output_1), next_state
    w = Stream('w')
    x = Stream('x')
    y, z = f_multi_window(
        in_streams=[w,x], num_out_streams=2,
        window_size=2, step_size=2, state=0)
    
    w.extend(range(10))
    x.extend(range(100, 120, 2))
    
    Stream.scheduler.step()
    assert recent_values(y) == [203, 216, 229, 242, 255]
    assert recent_values(z) == [103, 110, 117, 124, 131]
    

    
if __name__ == '__main__':
    test_try()

    
    
    
    
    

