"""
This code is a streaming version of code written by Zulko who
created the 'pianoputer.' All the ideas are from Zulko's
version. All we do here is show how to convert code for an
array into code for streams.

To play music, download sounddevice.

A problem encountered when switching from arrays to streams
is that code operating on an array can use metrics --- such as
maximum --- over the entire array, whereas code operating on
streams has to compute pitch shift based on the available
data up to that point. For the streaming version we assume
that the maximum over the entire array is 4096.0 (See the
last lines of stretch in which result is computed). A poor
assumption of the maximum may result in clipping or numerical
problems.

This code has both the original version (modified with max
assumed to be 4096) and the streaming version so that you
can see how one is converted into the other. speedx and
stretch are from the original version, while the method
Stretch.stretch is the streaming version.

The repository includes a short wav file called 'guitar.wav'
If you run test_pitchshift you will hear the sound shifted
to a lower pitch, then the original sound, and then the sound
shifted to a higher pitch. In each case you will first hear
the sound created by original version (modified by assuming
max is 4096) and the streaming version.

"""
import numpy as np
#!/usr/bin/env python
import sys
import os
from scipy.io import wavfile
from scipy.io.wavfile import read, write
import sounddevice as sd

sys.path.append(os.path.abspath("../../IoTPy/core"))
sys.path.append(os.path.abspath("../../IoTPy/agent_types"))
sys.path.append(os.path.abspath("../../IoTPy/helper_functions"))

from basics import map_wl, sink_w
from stream import StreamArray
from run import run
from sink import sink_window
from recent_values import recent_values

def speedx(sound_array, factor):
    """ Multiplies the sound's speed by some `factor` """
    indices = np.round( np.arange(0, len(sound_array), factor) )
    indices = indices[indices < len(sound_array)].astype(int)
    return sound_array[ indices.astype(int) ]


def stretch(sound_array, f, window_size, h):
    """ Stretches the sound by a factor `f` """
    phase  = np.zeros(window_size)
    hanning_window = np.hanning(window_size)
    result = np.zeros( int(len(sound_array) /f + window_size))

    window_size = int(window_size)
    for i in np.arange(0, len(sound_array)-(window_size+h), int(h*f)):

        # two potentially overlapping subarrays
        a1 = sound_array[i: i + int(window_size)]
        a2 = sound_array[i + h: i + int(window_size) + int(h)]

        # resynchronize the second array on the first
        s1 =  np.fft.fft(hanning_window * a1)
        s2 =  np.fft.fft(hanning_window * a2)
        phase = (phase + np.angle(s2/s1)) % 2*np.pi
        a2_rephased = np.fft.ifft(np.abs(s2)*np.exp(1j*phase))

        # add to result
        i2 = int(i/f)
        result[i2 : i2 + window_size] += np.real((hanning_window*a2_rephased))
        
    #result = ((2**(16-4)) * result/result.max()) # normalize
    # Assume result.max() is 2**(16-4)x
    return result.astype('int16')

def pitchshift(snd_array, n, window_size=2**13, h=2**11):
    """ Changes the pitch of a sound by ``n`` semitones. """
    factor = 2**(1.0 * n / 12.0)
    stretched = stretch(snd_array, 1.0/factor, window_size, h)
    return speedx(stretched[window_size:], factor)

def pitchshift_stream(snd_array, n, window_size=2**13, h=2**11):
    """ Changes the pitch of a sound by ``n`` semitones. """
    factor = 2**(1.0 * n / 12.0)
    
    stretched = stretch(snd_array, 1.0/factor, window_size, h)
    return speedx(stretched[window_size:], factor)

    x = StreamArray('x', dtype=np.int16)
    y = StreamArray('y', dtype=np.int16)
    stretch_object = Stretch(
        in_stream=x, out_stream=y, tone=-12, N=2**13, M=2**11)
    x.extend(guitar_sound)
    run()
    return speedx(x.recent[window_size:], factor)

class Stretch(object):
    """
    Parameters
    __________

    """
    def __init__(self, in_stream, out_stream, f, window_size, h):
        self.in_stream = in_stream
        self.out_stream = out_stream
        self.f = f
        self.window_size = window_size
        self.h = h
        self.phase = np.zeros(window_size)
        self.hanning_window = np.hanning(self.window_size)
        self.result = np.zeros(window_size+h)
        sink_window(
            func=self.stretch, in_stream=self.in_stream,
            window_size=self.window_size+self.h,
            step_size=int(h*f))
    def stretch(self, window):
        # two potentially overlapping subarrays
        a1 = window[:self.window_size]
        a2 = window[int(self.h): self.window_size+int(self.h)]

        # resynchronize the second array on the first
        s1 = np.fft.fft(self.hanning_window * a1)
        s2 = np.fft.fft(self.hanning_window * a2)
        self.phase = (self.phase + np.angle(s2/s1)) % 2*np.pi
        a2_rephased = np.fft.ifft(np.abs(s2)*np.exp(1j*self.phase))

        # add to result
        self.result[self.h : self.h + self.window_size] += np.real(
            (self.hanning_window*a2_rephased))
        current_output = (self.result[:self.h]).astype('int16')
        self.result = np.roll(self.result, -self.h)
        self.result[self.h:] = 0.0
        self.out_stream.extend(current_output)
                 
def test_pitchshift():
    fps, snd_array = wavfile.read("./guitar.wav")
    n = -12
    output = pitchshift(snd_array, n)
    sd.play(output, blocking=True)
    tones = [-12, 0, 12]
    for n in tones:
        # Original code
        transposed = pitchshift(snd_array, n)
        # Streaming code
        transposed_stream = pitchshift_stream(snd_array, n)
        sd.play(transposed, blocking=True)
        sd.play(transposed_stream, blocking=True)
        
        
if __name__ == '__main__':
    test_pitchshift()
    
