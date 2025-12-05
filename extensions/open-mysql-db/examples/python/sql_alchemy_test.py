import unittest


class MyTestCase(unittest.TestCase):
    def test_sqlalchemy_pymysql(self):
        from sqlalchemy import create_engine, text
        engine = create_engine("mysql+pymysql://root:4pdadmin@127.0.0.1:3307/demo_db", echo=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM demo_table1"))
            for row in result:
                print(row)


if __name__ == '__main__':
    unittest.main()
