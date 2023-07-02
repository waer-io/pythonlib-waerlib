import waerlib
import pandas as pd

if __name__ == '__main__':
    df = pd.DataFrame([])
    df.loc[:,'timestamp'] = pd.to_datetime([
        '2020-01-01T00:10:00.100001','2020-01-02T00:20:00.100001','2020-01-03T00:40:00.100001',
        '2020-01-01T00:50:00.100001','2020-01-02T01:00:00.100001','2020-01-03T01:10:00.100001'
    ])
    df.loc[:,'key'] = ['sleep','activity','sleep','activity','sleep','activity']
    df.loc[:,'val'] = [100,200,300,400,500,600]
    df.loc[:,'version'] = 'test'
    df = waerlib.write('123', df, 'parsed')
