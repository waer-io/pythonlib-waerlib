import os
import waerlib

if __name__ == '__main__':
    df = waerlib.read('123','','','','localhost','5000','admin','password123')
    print(df)
    import ipdb
    ipdb.set_trace()

