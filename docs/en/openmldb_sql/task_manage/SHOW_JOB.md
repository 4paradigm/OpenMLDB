# SHOW JOB

The `SHOW JOB` statement is used to display the details of a single job that has been submitted according to the given JOB ID.

```SQL
SHOW JOB job_id;
```


## Example

Submit an online data import task:

```sql
LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');
```
The output is shown below. The job id of the above command is 1.
```sql
-------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  job_id   job_type            state       start_time            end_time   parameter                       cluster    application_id   error   db     name   pid    cur_task   component
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  1        ImportOfflineData   Submitted   2023-05-30 19:54:13              /tmp/sql-14193917739024710575   local[*]                            NULL   NULL   NULL   NULL       TaskManager
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
```

Check the job whose Job ID is 1:
```sql
SHOW JOB 1;

 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  job_id   job_type            state       start_time            end_time   parameter                       cluster    application_id   error   db     name   pid    cur_task   component
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
  1        ImportOfflineData   Submitted   2023-05-30 19:54:13              /tmp/sql-14193917739024710575   local[*]                            NULL   NULL   NULL   NULL       TaskManager
 -------- ------------------- ----------- --------------------- ---------- ------------------------------- ---------- ---------------- ------- ------ ------ ------ ---------- -------------
```

## Related Sentences

[SHOW JOBS](./SHOW_JOBS.md)

[STOP JOBS](./STOP_JOB.md)
