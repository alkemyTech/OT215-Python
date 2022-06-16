import pandas as pd

def cleaningData():

    rawUMoron = pd.read_csv('/home/lautaror/airflow/dags/files/queryUMoron.csv')
    rawUNRC = pd.read_csv('/home/lautaror/airflow/dags/files/queryUNRC.csv')
    postalCodes = pd.read_csv('/home/lautaror/airflow/dags/files/codigos_postales.csv')

    postalCodes = postalCodes.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'})
    postalCodes['postal_code'] = postalCodes['postal_code'].astype('str')
    postalCodes['location'] = postalCodes['location'].str.lower().str.replace("-"," ").str.strip()

    rawUMoron['university'] = rawUMoron['university'].str.lower().str.replace("-"," ").str.strip()
    rawUMoron['career'] = rawUMoron['career'].str.lower().str.replace("-"," ").str.strip()
    rawUMoron['name'] = rawUMoron['name'].str.replace("-"," ").str.replace("Mr."," ").str.replace("Mrs."," ").str.replace("Miss."," ").str.replace("Ms."," ").str.replace("Dr."," ").str.replace("phd"," ").str.replace("md"," ").str.replace("dvm"," ").str.replace("jr."," ").str.replace("dds"," ").str.replace("."," ").str.replace("iii"," ") .str.lower().str.strip()
    name = rawUMoron['name'].str.split(' ', expand=True)
    name.columns = ['first_name', 'last_name']
    rawUMoron = pd.concat([rawUMoron, name], axis=1)
    rawUMoron = rawUMoron.drop(['name', 'Unnamed: 0'], axis=1)
    rawUMoron['email'] = rawUMoron['email'].str.lower().str.replace("-"," ").str.strip()
    rawUMoron['gender'] = rawUMoron['gender'].replace('M', 'male').replace('F', 'female')
    rawUMoron['inscription_date'] = pd.to_datetime(rawUMoron['inscription_date'], format='%Y-%m-%d')
    rawUMoron['postal_code'] = rawUMoron['postal_code'].astype('str')
    dfUMoron = pd.merge(rawUMoron, postalCodes, on='postal_code')
    dfUMoron.to_csv('/home/lautaror/airflow/dags/files/dataUMoron.txt', index=False)

    rawUNRC['university'] = rawUNRC['university'].str.lower().str.replace("-"," ").str.strip()
    rawUNRC['career'] = rawUNRC['career'].str.lower().str.replace("-"," ").str.strip()
    rawUNRC['names'] = rawUNRC['names'].str.replace("-"," ").str.replace("Mr."," ").str.replace("Mrs."," ").str.replace("Miss."," ").str.replace("Ms."," ").str.replace("Dr."," ").str.replace("phd"," ").str.replace("md"," ").str.replace("dvm"," ").str.replace("jr."," ").str.replace("dds"," ").str.replace("."," ").str.lower().str.strip()
    names = rawUNRC['names'].str.split(' ', expand=True)
    names.columns = ['first_name', 'last_name']
    rawUNRC = pd.concat([rawUNRC, names], axis=1)
    rawUNRC = rawUNRC.drop(['names', 'Unnamed: 0'], axis=1)
    rawUNRC['email'] = rawUNRC['email'].str.lower().str.replace("-"," ").str.strip()
    rawUNRC['gender'] = rawUNRC['gender'].replace('M', 'male').replace('F', 'female')
    rawUNRC['inscription_date'] = pd.to_datetime(rawUNRC['inscription_date'], format='%Y-%m-%d')
    rawUNRC['location'] = rawUNRC['location'].str.lower().str.replace("-"," ").str.strip()
    dfUNRC = pd.merge(rawUNRC, postalCodes, on='location')
    dfUNRC.to_csv('/home/lautaror/airflow/dags/files/dataUNRC.txt', index=False)

