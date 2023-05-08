import waerlib
import pandas as pd

if __name__ == '__main__':
    df = pd.DataFrame([])
    df.loc[:,'user_id'] = ['123','123','123','234','234','234']
    df.loc[:,'timestamp'] = ['2020-01-01','2020-01-02','2020-01-03','2020-01-01','2020-01-02','2020-01-03']
    df.loc[:,'key'] = ['sleep','activity','sleep','activity','sleep','activity']
    df.loc[:,'value'] = [100,200,300,400,500,600]
    df.timestamp = pd.to_datetime(df.timestamp)
    df = waerlib.write(df,'waer-stg-datalake','terra')
    
