import pandas as pd

# University data processing.
def data_processing(*args):
	for index, file in enumerate(args):
		df_university_data = pd.read_csv(f"airflow/dags/files/{file}")

		# Database union and data normalization.
		df_external_data = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTZuCecDicz0IrY7YzSDeQebtTjJ40izeNVS_vW9Kak6kt2BRTTdXC1sqJLdxM7c5amRdRgr5CngZZM/pub?gid=1807168318&single=true&output=csv")
		df_external_data.columns = ["postal_code","location"]
		df_external_data= df_external_data.drop_duplicates('postal_code')
		df_external_data.reset_index(inplace=True)
		if "postal_code" in df_university_data.columns:
			df_university_data = pd.merge(df_university_data, df_external_data, on="postal_code")
			df_university_data["postal_code"] = df_university_data["postal_code"].apply(str)
		else:
			df_university_data["location"] = df_university_data["location"].str.upper()
			df_university_data = pd.merge(df_university_data, df_external_data, on="location")	
		df_university_data["location"] = df_university_data["location"].str.lower().str.replace("_", " ").str.strip()

		# Name separation and data normalization.
		df_university_data["full_name"] = df_university_data["full_name"].str.lower()
		corrupted_data = ["dr.", "ms.", "md", "mr.", "mrs.", "dvm", "dds", "miss"]
		for index, value in enumerate(corrupted_data):
			df_university_data["full_name"] = df_university_data["full_name"].str.replace(value,"")
		df_university_data["full_name"] = df_university_data["full_name"].str.replace("_", " ").str.strip()
		name = df_university_data["full_name"].str.split(" ", n = 1, expand = True) 
		df_university_data["first_name"] = name[0] 
		df_university_data["last_name"] = name[1] 
		df_university_data.drop(columns =["full_name"], inplace = True)

		# Data normalization.
		df_university_data["university"] = df_university_data["university"].str.lower().str.replace("_", " ").str.strip()
		df_university_data["career"] = df_university_data["career"].str.lower().str.replace("_", " ").str.strip()
		df_university_data["inscription_date"] = df_university_data["inscription_date"].apply(str)
		df_university_data["gender"] = df_university_data["gender"].str.lower().str.replace("m","male").str.replace("f","female")
		df_university_data["age"] = pd.to_numeric(df_university_data["age"], downcast='integer')
		df_university_data["email"] = df_university_data["email"].str.lower().str.strip()

		# Data reorganization.
		df_university_data=df_university_data[["university", "career", "inscription_date", "first_name", "last_name", "gender", "age", "postal_code", "location", "email"]]
		
		# Save text file.
		name_u = file.strip(".csv").split("_")
		df_university_data.to_csv(f"airflow/dags/files/{name_u[0]}_normalized.txt")
