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
 
-------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  job_id   job_type            state       start_time            end_time   parameter                       cluster    application_id   error   db     name   pid    cur_task   component
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  1        ImportOfflineData   Submitted   2023-05-30 19:54:13              /tmp/sql-14193917739024710575   local[*]                            NULL   NULL   NULL   NULL       TaskManager
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
```

View all current tasks again:

```sql
SHOW JOBS;

-------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  job_id   job_type            state       start_time            end_time   parameter                       cluster    application_id   error   db     name   pid    cur_task   component
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  1        ImportOfflineData   Submitted   2023-05-30 19:54:13              /tmp/sql-14193917739024710575   local[*]                            NULL   NULL   NULL   NULL       TaskManager
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------

 1 row in set
```

show a job from TaskManager with specified id
```sql
SHOW JOBS FROM TASKMANAGER like '1';

-------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  job_id   job_type            state       start_time            end_time   parameter                       cluster    application_id   error   db     name   pid    cur_task   component
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  1        ImportOfflineData   Submitted   2023-05-30 19:54:13              /tmp/sql-14193917739024710575   local[*]                            NULL   NULL   NULL   NULL       TaskManager
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------

 1 row in set
```

### Get Jobs From NameServer
Get all jobs from NameServer in current database
```sql
show jobs from NameServer;
  -------- ----------- ---------- --------------------- --------------------- ----------- --------- ---------------- ------- ------ ------ ----- ---------- ------------
  job_id   job_type    state      start_time            end_time              parameter   cluster   application_id   error   db     name   pid   cur_task   component
 -------- ----------- ---------- --------------------- --------------------- ----------- --------- ---------------- ------- ------ ------ ----- ---------- ------------
  2        kDeployOP   FINISHED   2023-05-29 17:03:12   2023-05-29 17:03:17   NULL        NULL      NULL             NULL    test   t1     0     -          NameServer
 -------- ----------- ---------- --------------------- --------------------- ----------- --------- ---------------- ------- ------ ------ ----- ---------- ------------

1 rows in set
```
Get a job from NameServer with specified id
```sql
show jobs from nameserver like '2';
  -------- ----------- ---------- --------------------- --------------------- ----------- --------- ---------------- ------- ------ ------ ----- ---------- ------------
  job_id   job_type    state      start_time            end_time              parameter   cluster   application_id   error   db     name   pid   cur_task   component
 -------- ----------- ---------- --------------------- --------------------- ----------- --------- ---------------- ------- ------ ------ ----- ---------- ------------
  2        kDeployOP   FINISHED   2023-05-29 17:03:12   2023-05-29 17:03:17   NULL        NULL      NULL             NULL    test   t1     0     -          NameServer
 -------- ----------- ---------- --------------------- --------------------- ----------- --------- ---------------- ------- ------ ------ ----- ---------- ------------

1 rows in set
```

## Related Sentences

[SHOW JOB](./SHOW_JOB.md)

[STOP JOBS](./STOP_JOB.md)
