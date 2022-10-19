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
---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

Check the job whose Job ID is 1:
```sql
SHOW JOB 1;

---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

## Related Sentences

[SHOW JOBS](./SHOW_JOBS.md)

[STOP JOBS](./STOP_JOB.md)
