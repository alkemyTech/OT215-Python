import pandas as pd


def normal_str(s: str):
    return s.replace('-', ' ').lower().strip()


# lista de columnas a normalizar strings
uai_to_norm = ['university', 'career',
               'full_name', 'email', 'location']

unlpam_to_norm = ['university', 'career',
                  'first_name', 'last_name', 'email']

words = ['Mr.', 'Dr.', 'Ms.', 'Mrs.', 'dvm', 'dds', 'jr.', 'md']

# df con los codigos postales
df_pc = pd.read_csv('files/codigos_postales.csv',
                    names=['postal_code', 'location'], dtype=str, header=0)


def uai_cleaner():

    df_uai = pd.read_csv('files/query_uai.csv', index_col=0)

    df_pc['location'] = df_pc['location'].apply(normal_str)
    df_pc.drop_duplicates(subset='location', keep='first', inplace=True)

    # los nombres con mas de dos palabras los trato aparte
    mask = df_uai['full_name'].str.split('-').map(len) > 2
    bad_names = df_uai.loc[mask, 'full_name']

    # quitamos la palabra conflictiva
    for word in words:
        bad_names = bad_names.apply(lambda x: x.replace(word, ''))

    # actualizamos el df con los nombres corregidos
    df_uai.loc[mask, 'full_name'] = bad_names

    # normalizamos los strings
    df_uai[uai_to_norm] = df_uai[uai_to_norm].applymap(normal_str)

    # separamos el nombre en dos partes
    df_uai['first_name'] = df_uai['full_name'].apply(lambda x: x.split()[0])
    df_uai['last_name'] = df_uai['full_name'].apply(lambda x: x.split()[1])
    df_uai.drop('full_name', axis='columns', inplace=True)

    # YYYY-MM-DD (%Y-%m-%d) <- formato default cuando se pasa a str
    df_uai['inscription_date'] = pd.to_datetime(
        df_uai['inscription_date'], format="%y/%b/%d"
    ).astype(str)

    df_uai.loc[df_uai['gender'] == 'M', 'gender'] = 'male'
    df_uai.loc[df_uai['gender'] == 'F', 'gender'] = 'female'

    df_full = pd.merge(df_uai, df_pc, on='location', how='left')
    df_full.to_csv('files/uai_clean.txt')


def unlpam_cleaner():

    df_unlpam = pd.read_csv('files/query_unlpam.csv', index_col=0,
                            dtype={'postal_code': str})

    df_pc['location'] = df_pc['location'].apply(normal_str)

    # YYYY-MM-DD (%Y-%m-%d) <- formato default cuando se pasa a str
    df_unlpam['inscription_date'] = pd.to_datetime(
        df_unlpam['inscription_date'], format="%d/%m/%Y"
    ).astype(str)

    df_unlpam.loc[df_unlpam['gender'] == 'M', 'gender'] = 'male'
    df_unlpam.loc[df_unlpam['gender'] == 'F', 'gender'] = 'female'

    df_unlpam[unlpam_to_norm] = df_unlpam[unlpam_to_norm].applymap(normal_str)

    df_full = pd.merge(df_unlpam, df_pc, on='postal_code', how='left')
    df_full.to_csv('files/unlpam_clean.txt')


def uai_unlpam_cleaner():
    uai_cleaner()
    unlpam_cleaner()

