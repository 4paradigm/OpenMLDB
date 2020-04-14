#include"common.h"
int daemon_quit = 0;
struct settings settings = {16};
void settings_init(void)
{
    settings.port = 7900;
    /* By default this string should be NULL for getaddrinfo() */
    settings.inter = NULL;
    settings.item_buf_size = 4 * 1024;     /* default is 4KB */
    settings.maxconns = 1024;         /* to limit connections-related memory to about 5MB */
    settings.verbose = 0;
    settings.num_threads = 16;
    settings.flush_limit = 1024; // 1M
    settings.flush_period = 60 * 10; // 10 min
    settings.slow_cmd_time = 0.1; // 100ms
    settings.max_bucket_size  = (uint32_t)(4000 << 20); // 4G
    settings.check_file_size = false;
    settings.autolink = true;
}

