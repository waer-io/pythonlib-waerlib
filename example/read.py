import os
import waerlib

if __name__ == '__main__':
    df = waerlib.read('123','2020-01-01 00:20','2020-01-02 00:10',['activity', 'sleep'], 'parsed')
