import pandas as pd

def normalize_fun(path_file):
    df = pd.read_csv(path_file)
    df['university'] = df['university'].str.lower()
    df['university'] = df['university'].apply(lambda x: x.replace("-",""))
    # idem for career,first_name,last_name,location,email
    # df[]