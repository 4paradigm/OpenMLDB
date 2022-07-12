# STOP JOB

```SQL
STOP JOB;
```

The `STOP JOB` statement stops a single job that has already been submitted.

## Example

Submit an online data import task:

```
LOAD DATA INFIEL 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');

 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFIEL 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

Stop with Job ID 1:

```
STOP JOB 1;

---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   STOPPED     0            1641981373227   LOAD DATA INFIEL 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

## Related Sentences

[SHOW JOBS](./SHOW_JOBS.md)

[SHOW JOB](./SHOW_JOB.md)
