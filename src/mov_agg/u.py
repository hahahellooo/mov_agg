import pandas as pd

def merge(load_dt='20240724'):
    read_df = pd.read_parquet('~/tmp/test_parquet')
    cols = ['movieCd','movieNm','openDt','audiCnt',
            'load_dt','multiMovieYn','repNationCd']
    df = read_df[cols]
    #print(df.head(3))
    dw = df[(df['movieCd'] =='20247781') & (df['load_dt'] == int(load_dt))].copy()
    print(dw)
    print(dw.dtypes)
    
    # 카테고리타입 object
    dw['load_dt'] = dw['load_dt'].astype('object')
    dw['multiMovieYn'] = dw['multiMovieYn'].astype('object')
    dw['repNationCd'] = dw['repNationCd'].astype('object')
    print(dw.dtypes)

    # null 값 unknown으로 변환
    dw['multiMovieYn'] = dw['multiMovieYn'].fillna('unknown')
    dw['repNationCd'] = dw['repNationCd'].fillna('unknown')
    print(dw.dtypes)
    print(dw)

    # merge
    u_mul = dw[dw['multiMovieYn'] == 'unknown']
    u_nat = dw[dw['repNationCd'] == 'unknown']
    m_df = pd.merge(u_mul, u_nat, on='movieCd', suffixes=('_m', '_n'))

    print("머지 DF")
    print(m_df)
    return m_df

