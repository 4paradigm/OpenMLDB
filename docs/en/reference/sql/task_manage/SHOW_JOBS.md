# SHOW JOBS

```SQL
SHOW JOBS;
```

The `SHOW JOBS` statement displays a list of tasks that have been submitted.

## Example

View all current tasks:

```
SHOW JOBS;

 ---- ---------- ------- ------------ ---------- ----------- --------- ---------------- -------
  id   job_type   state   start_time   end_time   parameter   cluster   application_id   error
 ---- ---------- ------- ------------ ---------- ----------- --------- ---------------- -------
```

Submit an online data import task:

```
LOAD DATA INFIEL 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');

---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFIEL 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

View all current tasks:

```
SHOW JOBS;

---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            1641981373227   LOAD DATA INFIEL 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------

 1 row in set
```

## Related Sentences

[SHOW JOB](./SHOW_JOB.md)

[STOP JOBS](./STOP_JOB.md)
