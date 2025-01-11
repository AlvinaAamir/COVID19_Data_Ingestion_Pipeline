# %%
import requests
import pandas as pd
import sqlalchemy as sa
import pyodbc

# %%
resource_id = '455fd63b-603d-4608-8216-7d8647f43350'
Nrecords = 10000
api_url = 'https://data.ontario.ca/api/3/action/datastore_search?resource_id={}&limit={}'.format(resource_id, Nrecords)
print(api_url)

# %%
headers = {'user-agent' : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}

# %%
response = requests.get(url=api_url, headers=headers)
response

# %%
data = response.json()
data

# %%
# What parameters do you need to provide?
# For this API, the parameters are: resource_id: The unique identifier of the dataset, limit: The number of records to fetch.


# %%
# What type of authentication do you need to use?
# This particular API appears to be a public API that does not require authentication.

# %%
# Write a function to request the data
def get_covid_data(resource_id, limit=100):

    api_url = f'https://data.ontario.ca/api/3/action/datastore_search?resource_id={resource_id}&limit={limit}'
    headers = {'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}
    
    response = requests.get(url=api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# %%
resource_id = '455fd63b-603d-4608-8216-7d8647f43350'
data = get_covid_data(resource_id)
print(data)

# %%
data.keys()
data['result'].keys()

# %%
pd.DataFrame(data['result']['records'])

# %%
covid_positive = pd.DataFrame(data['result']['records'])
covid_positive

# %%
# Data Cleaning
covid_positive['Case_Reported_Date'] = pd.to_datetime(covid_positive['Case_Reported_Date'])
covid_positive['Accurate_Episode_Date'] = pd.to_datetime(covid_positive['Accurate_Episode_Date'])
covid_positive['Test_Reported_Date'] = pd.to_datetime(covid_positive['Test_Reported_Date'])
covid_positive['Specimen_Date'] = pd.to_datetime(covid_positive['Specimen_Date'])
covid_positive.head(10)

# %%
# Extracting Month and Year from reported date
covid_positive['Case_Reported_Year'] = covid_positive['Case_Reported_Date'].dt.year
covid_positive['Case_Reported_Month'] = covid_positive['Case_Reported_Date'].dt.month

# %%
# One-hot encoding for gender
covid_positive['GenderMale'] = covid_positive['Client_Gender'].map({'MALE': 1, 'FEMALE': 0})

# %%
# Replace missing values in Outcome1 with 'NON FATAL'
covid_positive['Outcome1'].fillna('NON FATAL', inplace=True)
# Create a binary indicator for fatality
covid_positive['Fatal'] = covid_positive['Outcome1'].apply(lambda x: 1 if x == 'FATAL' else 0)

# %%
# Convert Age_Group to new age categories
def categorize_age_group(age_group):
    if age_group == '90+':
        return 4  # 70+ years
    elif age_group == '<20':
        return 1  # 0-29 years
    else:
        age_numeric = int(age_group.strip('s'))
        if age_numeric < 30:
            return 1  # 0-29 years
        elif age_numeric < 50:
            return 2  # 30-49 years
        elif age_numeric < 70:
            return 3  # 50-69 years
        else:
            return 4  # 70+ years

covid_positive['Age_Category'] = covid_positive['Age_Group'].apply(categorize_age_group)
covid_positive

# %%
final_cols = ['_id', 'Case_Reported_Year', 'Case_Reported_Month', 'GenderMale', 'Fatal', 'Age_Category']

# %%
covid_positive = covid_positive[final_cols]
covid_positive

# %%
# Checking NA
covid_positive.isna().sum()

# %%
# Database Connection
connection_url = sa.engine.URL.create(
    drivername = "mssql+pyodbc",
    username   = "aimerliu",
    password   = "2024!Schulich",
    host       = "mban2024-ms-sql-server.c1oick8a8ywa.ca-central-1.rds.amazonaws.com",
    port       = "1433",
    database   = "aimerliu_db",
    query = {
        "driver" : "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate" : "yes"
    }
)

# %%
my_engine = sa.create_engine(connection_url)

# %%
# Ingest the data into your own database in our Microsoft SQL Server
covid_positive.to_sql(
    name='covid_postive',
    con=my_engine,
    schema = 'uploads',
    if_exists='replace',
    index=False,
    dtype= {
        '_id': sa.types.INTEGER,
        'Case_Reported_Year': sa.types.INTEGER,
        'Case_Reported_Month': sa.types.INTEGER,
        'GenderMale': sa.types.Boolean,
        'Fatal': sa.types.Boolean,
        'Age_Category': sa.types.INTEGER
    },
    method='multi'
)

# %%



