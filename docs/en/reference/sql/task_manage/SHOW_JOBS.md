# SHOW JOBS

**Syntax**
```sql
ShowJobsStatement ::=
    `SHOW JOBS` OptFromPathExpression OptLikeString

OptFromPathExpression ::=
    `FROM` ComponentIdentifier | /* Nothing */

ComponentIdentifier ::=
    `TASKMANAGER` |
    `NAMESERVER`

OptLikeString ::=
    `LIKE` string_literal
```

The `SHOW JOBS` statement displays a list of submitted tasks in the cluster version, including all kinds of jobs in TaskManager and Namerver

## Example

### Get Job From TaskManager

View all current tasks:

```sql
SHOW JOBS;

 ---- ---------- ------- ------------ ---------- ----------- --------- ---------------- -------
  id   job_type   state   start_time   end_time   parameter   cluster   application_id   error
 ---- ---------- ------- ------------ ---------- ----------- --------- ---------------- -------
```

Submit an online data import task:

```sql
LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');
 
---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

View all current tasks again:

```sql
SHOW JOBS;

---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------

 1 row in set
```

show a job from TaskManager with specified id
```sql
SHOW JOBS FROM TASKMANAGER like '1';

---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------

 1 row in set
```

### Get Jobs From NameServer
Get all jobs from NameServer in current database
```sql
show jobs from NameServer;
 -------- ------------- ------ ------ ----- -------- --------------------- --------------------- ----------
  job_id   op_type       db     name   pid   status   start_time            end_time              cur_task
 -------- ------------- ------ ------ ----- -------- --------------------- --------------------- ----------
  2        kAddIndexOP   test   t2     0     kDone    2023-05-25 12:06:05   2023-05-25 12:06:10   -
 -------- ------------- ------ ------ ----- -------- --------------------- --------------------- ----------

1 rows in set
```
Get a job from NameServer with specified id
```sql
show jobs from nameserver like '2';
 -------- ------------- ------ ------ ----- -------- --------------------- --------------------- ----------
  job_id   op_type       db     name   pid   status   start_time            end_time              cur_task
 -------- ------------- ------ ------ ----- -------- --------------------- --------------------- ----------
  2        kAddIndexOP   test   t2     0     kDone    2023-05-25 12:06:05   2023-05-25 12:06:10   -
 -------- ------------- ------ ------ ----- -------- --------------------- --------------------- ----------

1 rows in set
```

## Related Sentences

[SHOW JOB](./SHOW_JOB.md)

[STOP JOBS](./STOP_JOB.md)
