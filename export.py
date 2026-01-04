import pandas as pd

def export_excel(df, path="data/latest.xlsx"):
    df.to_excel(path, index=False)
