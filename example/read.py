import os
import waerlib

if __name__ == '__main__':
    df = waerlib.read('123','2019-12-01','2023-12-31', ['sleep','activity','sleep','activity','sleep','activity'], 'parsed')
    print(df)
