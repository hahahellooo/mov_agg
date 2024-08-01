from mov_agg.u import merge

def test_merge():
    df = merge()
    print("===============================")
    print(df)
    assert len(df) > 0
