import pandas as pd

def merge(load_dt='20240724'):
    read_df = pd.read_parquet('~/tmp/test_parquet')
    cols = ['movieCd','movieNm','openDt','audiCnt',
            'load_dt','multiMovieYn','repNationCd']
    df = read_df[cols]
    # 데이터 타입을 확인하고 정리할 자료를 간력하게 보기 위한 코드 
    dw = df[(df['movieCd'] =='20247781') & (df['load_dt'] == int(load_dt))].copy()
    print(dw)
    print(dw.dtypes)
    
    # 카테고리타입 object로 변경
    dw['load_dt'] = dw['load_dt'].astype('object')
    dw['multiMovieYn'] = dw['multiMovieYn'].astype('object')
    dw['repNationCd'] = dw['repNationCd'].astype('object')
    print(dw.dtypes)

    # null 값 unknown으로 변환
    dw['multiMovieYn'] = dw['multiMovieYn'].fillna('unknown')
    dw['repNationCd'] = dw['repNationCd'].fillna('unknown')
    print(dw.dtypes)
    print(dw)

    # merge/ suffixes를 추가하는 이유는 같은 행 2개를 
    # 이어붙이기 때문에 참고하기 위해서 이다. 
    u_mul = dw[dw['multiMovieYn'] == 'unknown']
    u_nat = dw[dw['repNationCd'] == 'unknown']
    m_df = pd.merge(u_mul, u_nat, on='movieCd', suffixes=('_m', '_n'))

    print("머지 DF")
    print(m_df)
    return m_df

