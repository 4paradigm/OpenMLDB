#ifdef DARWIN

/*
 * Bellow we provide an alternative for clock_gettime,
 * which is not implemented in Mac OS X.
 */
static inline int clock_gettime(int clock_id, struct timespec *ts)
{
    struct timeval tv;

    if (clock_id != CLOCK_REALTIME)
    {
        errno = EINVAL;
        return -1;
    }
    if (gettimeofday(&tv, NULL) < 0)
    {
        return -1;
    }
    ts->tv_sec = tv.tv_sec;
    ts->tv_nsec = tv.tv_usec * 1000;
    return 0;
}

#endif /* DARWIN */