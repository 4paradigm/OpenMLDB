import pandas as pd
from sqlalchemy import create_engine

if __name__ == '__main__':
    # Create a Pandas DataFrame (replace this with your actual data)
    data = {'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'score': [1.1, 2.2, 3.3],
            'ts': [pd.Timestamp.utcnow().timestamp(), pd.Timestamp.utcnow().timestamp(),
                   pd.Timestamp.utcnow().timestamp()],
            'dt': [pd.to_datetime('20240101', format='%Y%m%d'), pd.to_datetime('20240201', format='%Y%m%d'),
                   pd.to_datetime('20240301', format='%Y%m%d')],
            }
    df = pd.DataFrame(data)

    # Create a MySQL database engine using SQLAlchemy
    engine = create_engine('mysql+pymysql://root:root@127.0.0.1:3307/demo_db')

    # Replace 'username', 'password', 'host', and 'db_name' with your actual database credentials

    # Define the name of the table in the database where you want to write the data
    table_name = 'demo_table1'

    # Write the DataFrame 'df' into the MySQL table
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    # 'if_exists' parameter options:
    # - 'fail': If the table already exists, an error will be raised.
    # - 'replace': If the table already exists, it will be replaced.
    # - 'append': If the table already exists, data will be appended to it.

    print("Data written to MySQL table successfully!")
