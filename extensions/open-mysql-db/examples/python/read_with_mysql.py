#!/usr/bin/env python

from sqlalchemy import create_engine, text

def main():
    engine = create_engine("mysql+pymysql://root:root@127.0.0.1:3307/db1", echo=True)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM db1.t1"))
        for row in result:
            print(row)

if __name__ == "__main__":
  main()
