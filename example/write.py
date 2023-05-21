import waerlib
import pandas as pd

if __name__ == '__main__':
    df = pd.DataFrame([])
    df.loc[:,'timestamp'] = pd.to_datetime([
        '2020-01-01 00:10','2020-01-02 00:20','2020-01-03 00:40',
        '2020-01-01 00:50','2020-01-02 01:00','2020-01-03 01:10'
    ])
    df.loc[:,'key'] = ['sleep','activity','sleep','activity','sleep','activity']
    df.loc[:,'value'] = [100,200,300,400,500,600]
    df = waerlib.write('123', df, 'test')
