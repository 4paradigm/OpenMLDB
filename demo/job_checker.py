# replace with diag tool later
import sqlalchemy as db
ZK = '127.0.0.1:2181'
ZK_PATH = '/openmldb'
LOG_PATTERN = '/work/openmldb/taskmanager/bin/logs/job_{}_error.log'

engine = db.create_engine(f'openmldb:///?zk={ZK}&zkPath={ZK_PATH}')
connection = engine.connect()
rs = connection.execute('SHOW JOBS').fetchall()
print(f'get {len(rs)} jobs')
has_failed = False
for row in rs:
    if row[2] != 'FINISHED':
        has_failed = True
        with open(LOG_PATTERN.format(row[0])) as f:
            job_error = f.read()
        print(f'job failed, check the log of job {row[0]},{row[1]}:\n{job_error}')

assert not has_failed, f'job list\n{rs}'
print('all finished')
