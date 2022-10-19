# STOP JOB

The `STOP JOB` statement can stop a given job that has already been submitted according to the JOB ID.


```SQL
STOP JOB job_id;
```


## Example

Submit an online data import task. The output shows that the JOB ID of this task is 1.

```sql
LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');

 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

Stop the job whose Job ID is 1:

```sql
STOP JOB 1;

---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   STOPPED     0            1641981373227   LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

## Related Sentences

[SHOW JOBS](./SHOW_JOBS.md)

[SHOW JOB](./SHOW_JOB.md)
