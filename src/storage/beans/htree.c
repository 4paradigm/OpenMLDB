/*
 *  Beansdb - A high available distributed key-value storage system:
 *
 *      http://beansdb.googlecode.com
 *
 *  Copyright 2009 Douban Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Davies Liu <davies.liu@gmail.com>
 *      Hurricane Lee <hurricane1026@gmail.com>
 */
#include<sys/time.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <fcntl.h>
#include <ctype.h>
#include <inttypes.h>

#include "fnv1a.h"
#include "mfile.h"
#include "htree.h"
#include "codec.h"
#include "const.h"
#include "diskmgr.h"

const int BUCKET_SIZE = 16;
const int SPLIT_LIMIT = 64;
const int MAX_DEPTH = 8;
static const long long g_index[] = {0, 1, 17, 273, 4369, 69905, 1118481, 17895697, 286331153, 4581298449L};

const char HTREE_VERSION[] = "HTREE001";
#define TREE_BUF_SIZE 512
#define calc_max(a,b) ((a)>(b)?(a):(b))
#define INDEX(it) (0x0f & (keyhash >> ((7 - node->depth - tree->depth) * 4)))
#define ITEM_LENGTH(it) ((it)->ksz + sizeof(Item) - ITEM_PADDING)
#define HASH(it) ((it)->hash * ((it)->ver>0))
#define DATA_file_start(data) (&((data)->size))
#define DATA_HEAD_SIZE (int)(((char*)&(((Data*)0)->head)) - (char*)(0))
#define DATA_BLOCK_SIZE 256
#define DATA_BLOCK_SIZE_SMALL 1024

typedef struct t_data Data;
struct t_data
{
    Data *next;
    int size;
    int used;
    int count;
    Item head[0];
};


typedef struct t_node Node;
struct t_node
{
    uint16_t is_node:1;
    uint16_t valid:1;
    uint16_t depth:4;
    uint16_t flag:9;
    uint16_t hash;
    uint32_t count;
    Data *data;
};

struct t_hash_tree
{
    int depth;
    int pos;
    int height;
    int block_size;
    Node *root;
    Codec *dc;
    pthread_mutex_t lock;
    char buf[TREE_BUF_SIZE];

    uint32_t updating_bucket;
    HTree *updating_tree;
};


// forward dec
static void add_item(HTree *tree, Node *node, Item *it, uint32_t keyhash, bool enlarge);
static void remove_item(HTree *tree, Node *node, Item *it, uint32_t keyhash);
static void split_node(HTree *tree, Node *node);
static void merge_node(HTree *tree, Node *node);
static void update_node(HTree *tree, Node *node);

static inline bool check_version(Item *oldit, Item *newit, HTree *tree, uint32_t keyhash)
{
    if (abs(newit->ver) >= abs(oldit->ver))
        return true;
    else
    {
        char key[KEY_BUF_LEN];
        dc_decode(tree->dc, key, KEY_BUF_LEN, oldit->key, oldit->ksz);
        printf("BUG: bad version, oldv=%d, newv=%d, key=%s, keyhash = 0x%x, oldpos = %u\n",  oldit->ver, newit->ver, key, keyhash, oldit->pos);
        return false;
    }
}

static inline uint32_t get_pos(HTree *tree, Node *node)
{
    return (node - tree->root) - g_index[(int)node->depth];
}

static inline Node *get_child(HTree *tree, Node *node, int b)
{
    int i = g_index[node->depth + 1] + (get_pos(tree, node) << 4) + b;
    return tree->root + i;
}

static inline void init_data(Data *data, int size)
{
    data->next = NULL;
    data->count = 0;
    data->used = DATA_HEAD_SIZE;
    data->size = size;
 }

static inline Data *get_data(Node *node)
{
    return node->data;
}

static inline void set_data(Node *node, Data *data)
{
   node->data = data;
}

static inline void free_data(Node *node)
{
    Data *d, *d0;
    for (d = node->data; d != NULL; )
    {
        d0 = d;
        d = d->next;
        free(d0);
    }
    node->data = NULL;
}



static inline uint32_t key_hash(HTree *tree, Item *it)
{
    char buf[KEY_BUF_LEN];
    int n = dc_decode(tree->dc, buf, KEY_BUF_LEN, it->key, it->ksz);
    return fnv1a(buf, n);
}

static Item *create_item(HTree *tree, const char *key, int len, uint32_t pos, uint16_t hash, int32_t ver)
{
    Item *it = (Item*)tree->buf;
    it->pos = pos;
    it->ver = ver;
    it->hash = hash;
    it->ksz = dc_encode(tree->dc, it->key, TREE_BUF_SIZE - (sizeof(Item) - ITEM_PADDING), key, len);
    return it;
}

static void enlarge_pool(HTree *tree)
{
    int i;
    int old_size = g_index[tree->height];
    int new_size = g_index[tree->height + 1];

    printf("enlarge pool %d -> %d, new_height = %d\n", old_size, new_size, tree->height + 1);

    tree->root = (Node*)beans_safe_realloc(tree->root, sizeof(Node) * new_size);
    memset(tree->root + old_size, 0, sizeof(Node) * (new_size - old_size));
    for (i = old_size; i<new_size; i++)
        tree->root[i].depth = tree->height;

    tree->height++;
}

static void clear(Node *node)
{
    Data *data = (Data*)beans_safe_malloc(64);
    init_data(data, 64);
    set_data(node, data);

    node->is_node = 0;
    node->valid = 1;
    node->count = 0;
    node->hash = 0;
}

static void add_item(HTree *tree, Node *node, Item *it, uint32_t keyhash, bool enlarge)
{
    int it_len = ITEM_LENGTH(it);
    while (node->is_node)
    {
        node->valid = 0;
        node = get_child(tree, node, INDEX(it));
    }

    Data *data0 = get_data(node);
    Data *data;
    for (data = data0; data != NULL;  data = data->next)
    {
        Item *p = data->head;
        int i;
        for (i = 0; i<data->count; ++i, p = (Item*)((char*)p + ITEM_LENGTH(p)))
        {
            if (it->ksz == p->ksz &&
                    memcmp(it->key, p->key, it->ksz) == 0)
            {
                check_version(p, it, tree, keyhash);
                node->hash += (HASH(it) - HASH(p)) * keyhash;
                node->count += (it->ver > 0);
                node->count -= (p->ver > 0);
                memcpy(p, it, sizeof(Item)); // safe
                return;
            }
        }
    }

    Data *last = NULL;
    Data *llast = NULL;
    for (data = data0; data != NULL && data->size < data->used + it_len; llast = last, last = data, data = data->next)
        ;

    if (data == NULL)
    {
        if (last->used + it_len > tree->block_size)
        {
            int size = DATA_HEAD_SIZE + it_len;
            data = (Data*)beans_safe_malloc(size);
            init_data(data, size);
            last->next = data;
        }
        else
        {
            int size = calc_max(last->used + it_len, last->size);
            size = calc_min(size, tree->block_size);
            data = (Data*)beans_safe_realloc(last, size);
            data->size = size;

            if (llast)
                llast->next = data;
            else
                set_data(node, data);
        }
    }

    Item *p = (Item*)(((char*)data) + data->used);
    safe_memcpy(p, data->size - data->used, it, it_len);
    data->count++;
    data->used += it_len;
    node->count += (it->ver > 0);
    node->hash += keyhash * HASH(it);

    if (node->count > SPLIT_LIMIT)
    {
        if (node->depth == tree->height - 1)
        {
            if (enlarge && (tree->height + tree->depth < MAX_DEPTH) && (node->count > SPLIT_LIMIT * 4))
            {
                int pos = node - tree->root;
                enlarge_pool(tree);
                node = tree->root + pos; // reload
                split_node(tree, node);
            }
        }
        else
        {
            split_node(tree, node);
        }
    }
}

static void split_node(HTree *tree, Node *node)
{
    Node *child = get_child(tree, node, 0);
    int i;
    for (i = 0; i < BUCKET_SIZE; ++i)
        clear(child + i);

    Data *data0 = get_data(node);
    Data *data;
    for (data = data0; data != NULL; data = data->next)
    {
	    Item *it = data->head;
	    for (i = 0; i < data->count; ++i)
	    {
		    int32_t keyhash = key_hash(tree, it);
		    add_item(tree, child + INDEX(it), it, keyhash, false);
		    it = (Item*)((char*)it + ITEM_LENGTH(it));
	    }
    }

    free_data(node);

    node->is_node = 1;
    node->valid = 0;
}

static void remove_item(HTree *tree, Node *node, Item *it, uint32_t keyhash)
{
    while (node->is_node)
    {
        node->valid = 0;
        node = get_child(tree, node, INDEX(it));
    }

    Data *data0 = get_data(node);
    Data *data;
    Data *last = NULL;
    for (data = data0; data != NULL; last = data, data = data->next)
    {
        Item *p = data->head;
        int i;
        int p_len;
        for (i = 0; i < data->count; ++i, p = (Item*)((char*)p + p_len))
        {
            p_len = ITEM_LENGTH(p);
            if (it->ksz == p->ksz &&
                    memcmp(it->key, p->key, it->ksz) == 0)
            {
                data->count--;
                data->used -= p_len;
                node->count -= p->ver > 0;
                node->hash -= keyhash * HASH(p);
                if (data->count > 0)
                {
                    memmove(p, (char*)p + p_len, data->size - ((char*)p - (char*)data) - p_len);
                }
                else if (data != data0 && data->next != NULL) //neither first nor last
                {
                    last->next = data->next;
                    free(data);
                }
                return;
            }
        }
    }
}

static void merge_node(HTree *tree, Node *node)
{
    clear(node);

    Node *child = get_child(tree, node, 0);
    int i, j;
    for (i = 0; i < BUCKET_SIZE; ++i)
    {
        Data *data0 = get_data(child + i);
        Data *data;
        for (data = data0; data != NULL; data = data->next)
        {
            Item *it = data->head;
            for (j = 0; j < data->count; ++j, it = (Item*)((char*)it + ITEM_LENGTH(it)))
            {
                if (it->ver > 0) 
                {
                    add_item(tree, node, it, key_hash(tree, it), false);
                } // drop deleted items, ver < 0
            }
        }
        free_data(child + i);
    }
}

static void update_node(HTree *tree, Node *node)
{
    if (node->valid) return;

    int i;
    node->hash = 0;
    if (node->is_node)
    {
        Node *child = get_child(tree, node, 0);
        node->count = 0;
        for (i = 0; i < BUCKET_SIZE; i++)
        {
            update_node(tree, child+i);
            node->count += child[i].count;
        }
        for (i = 0; i < BUCKET_SIZE; i++)
        {
            if (node->count > SPLIT_LIMIT * 4)
            {
                node->hash *= 97;
            }
            node->hash += child[i].hash;
        }
    }
    node->valid = 1;

    // merge nodes
    if (node->count <= SPLIT_LIMIT)
    {
        merge_node(tree, node);
    }
}

static Item *get_item_hash(HTree *tree, Node *node, Item *it, uint32_t keyhash)
{
    while (node->is_node)
        node = get_child(tree, node, INDEX(it));

    Item *r = NULL;
    Data *data0 = get_data(node);
    Data *data;
    for (data = data0; data != NULL && r == NULL; data = data->next)
    {
        Item *p = data->head;
        int i;
        int p_len;
        for (i = 0; i<data->count; i++, p = (Item*)((char*)p + p_len))
        {
            p_len = ITEM_LENGTH(p);
            if (ITEM_LENGTH(it) == p_len &&
                    memcmp(it->key, p->key, it->ksz) == 0)
            {
                r = p;
                break;
            }
        }
    }
    return r;
}

static inline int hex2int(char b)
{
    if (('0'<=b && b<='9') || ('a'<=b && b<='f'))
    {
        return (b>='a') ?  (b-'a'+10) : (b-'0');
    }
    else
    {
        return -1;
    }
}

static uint16_t get_node_hash(HTree *tree, Node *node, const char *dir,
                              unsigned int *count)
{
    if (node->is_node && strlen(dir) > 0)
    {
        char i = hex2int(dir[0]);
        if (i >= 0)
        {
            return get_node_hash(tree, get_child(tree, node, i), dir + 1, count);
        }
        else
        {
            if(count) *count = 0;
            return 0;
        }
    }
    update_node(tree, node);
    if (count) *count = node->count;
    return node->hash;
}

static char *list_dir(HTree *tree, Node *node, const char *dir, const char *prefix)
{
    int dlen = strlen(dir);
    while (node->is_node && dlen > 0)
    {
        int b = hex2int(dir[0]);
        if (b >= 0 && b < BUCKET_SIZE)
        {
            node = get_child(tree, node, b);
            ++dir;
            --dlen;
        }
        else
        {
            return NULL;
        }
    }

    int bsize = 4096;
    char *buf = (char*)beans_safe_malloc(bsize);
    memset(buf, 0, bsize);
    int n = 0, i;
    if (node->is_node)
    {
        update_node(tree, node);

        Node *child = get_child(tree, node, 0);
        if (node->count > 100000 || (prefix == NULL && node->count > SPLIT_LIMIT * 4))
        {
            for (i = 0; i < BUCKET_SIZE; i++)
            {
                Node *t = child + i;
                n += safe_snprintf(buf + n, bsize - n, "%x/ %u %u\n",
                              i, t->hash, t->count);
            }
        }
        else
        {
            for (i = 0; i < BUCKET_SIZE; i++)
            {
                char *r = list_dir(tree, child + i, "", prefix);
                int rl = strlen(r) + 1;
                if (bsize - n < rl)
                {
                    bsize += rl;
                    buf = (char*)beans_safe_realloc(buf, bsize);
                }
                n += safe_snprintf(buf + n, bsize - n, "%s", r);
                free(r);
            }
        }
    }
    else
    {
        char pbuf[20], key[KEY_BUF_LEN];
        int prefix_len = 0;
        if (prefix != NULL) 
            prefix_len = strlen(prefix);

        Data *data0 = get_data(node);
        Data *data;
        for (data = data0; data != NULL;  data = data->next)
        {
            Item *it = data->head;
            for (i = 0; i < data->count; i++, it = (Item*)((char*)it + ITEM_LENGTH(it)))
            {
                if (dlen > 0)
                {
                    safe_snprintf(pbuf, 20, "%08x", key_hash(tree, it));
                    if (memcmp(pbuf + tree->depth + node->depth, dir, dlen) != 0)
                    {
                        continue;
                    }
                }
                int l = dc_decode(tree->dc, key, KEY_BUF_LEN, it->key, it->ksz);
                if (prefix == NULL || (l >= prefix_len && strncmp(key, prefix, prefix_len) == 0))
                {
                    if (bsize - n < KEY_BUF_LEN + 32)
                    {
                        bsize *= 2;
                        buf = (char*)beans_safe_realloc(buf, bsize);
                    }

                    n += safe_snprintf(buf + n, bsize - n, "%s %u %d\n", key, it->hash, it->ver);
                }
            }
        }
    }
    return buf;
}

static void visit_node(HTree *tree, Node *node, fun_visitor visitor, void *param)
{
    int i;
    if (node->is_node)
    {
        Node *child = get_child(tree, node, 0);
        for (i = 0; i < BUCKET_SIZE; i++)
        {
            visit_node(tree, child + i, visitor, param);
        }
    }
    else
    {
        Data *data0 = get_data(node);
        Data *data;
        for (data = data0; data != NULL; data = data->next)
        {
            Item *p = data->head;
            Item *it = (Item*)tree->buf;
            int buf_size = TREE_BUF_SIZE - (sizeof(Item) - ITEM_PADDING);;
            int decode_len = 0;  // NOLINT
            for (i = 0; i < data->count; i++, p = (Item*)((char*)p + ITEM_LENGTH(p)))
            {
                safe_memcpy(it, buf_size, p, sizeof(Item));
                decode_len = dc_decode(tree->dc, it->key, buf_size, p->key, p->ksz);
                it->ksz = strlen(it->key);
                visitor(it, param);
            }
        }
    }
}

/*
 * API
 */

HTree *ht_new(int depth, int pos, bool tmp)
{
    HTree *tree = (HTree*)beans_safe_malloc(sizeof(HTree));
    memset(tree, 0, sizeof(HTree));
    tree->depth = depth;
    tree->pos = pos;
    tree->height = 1;
    if (tmp)
        tree->block_size = DATA_BLOCK_SIZE_SMALL;
    else
        tree->block_size = DATA_BLOCK_SIZE;
    tree->updating_bucket = -1;
    tree->updating_tree = NULL;

    int pool_size = g_index[tree->height];
    Node *root = (Node*)beans_safe_malloc(sizeof(Node) * pool_size);

    memset(root, 0, sizeof(Node) * pool_size);

    // init depth
    int i,j;
    for (i = 0; i < tree->height; i++)
    {
        for (j = g_index[i]; j < g_index[i + 1]; j++)
        {
            root[j].depth = i;
        }
    }

    tree->root = root;
    clear(tree->root);

    tree->dc = dc_new();
    pthread_mutex_init(&tree->lock, NULL);

    return tree;
}

HTree *ht_open(int depth, int pos, const char *path)
{
    char version[sizeof(HTREE_VERSION) + 1] = {0};
    HTree *tree = NULL;
    Node *root = NULL;
    int pool_used = 0;
    char *buf = NULL;

    FILE *f = fopen(path, "rb");
    if (f == NULL)
    {
        printf("open %s failed\n", path);
        return NULL;
    }

    if (fread(version, sizeof(HTREE_VERSION), 1, f) != 1
            || memcmp(version, HTREE_VERSION, sizeof(HTREE_VERSION)) != 0)
    {
        printf("the version %s is not expected\n", version);
        fclose(f);
        return NULL;
    }

    off_t fsize = 0;
    if (fread(&fsize, sizeof(fsize), 1, f) != 1 ||
            fseeko(f, 0, 2) != 0 || ftello(f) != fsize)
    {
        printf("the size %lld is not expected\n", (long long int)fsize);
        fclose(f);
        return NULL;
    }
    fseeko(f, sizeof(HTREE_VERSION) + sizeof(off_t), 0);
#if _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L
    if (posix_fadvise(fileno(f), 0, fsize, POSIX_FADV_SEQUENTIAL) != 0)
    {
        printf("posix_favise() failed");
    }
#endif

    tree = (HTree*)beans_safe_malloc(sizeof(HTree));

    memset(tree, 0, sizeof(HTree));
    tree->depth = depth;
    tree->pos = pos;
    tree->updating_bucket = -1;
    tree->block_size = DATA_BLOCK_SIZE;

    if (fread(&tree->height, sizeof(int), 1, f) != 1 ||
            tree->height + depth < 0 || tree->height + depth > 9)
    {
        printf("invalid height: %d\n", tree->height);
        goto FAIL;
    }

    int pool_size = g_index[tree->height];
    int psize = sizeof(Node) * pool_size;
    root = (Node*)beans_safe_malloc(psize);

    if (fread(root, psize, 1, f) != 1)
    {
        goto FAIL;
    }
    tree->root = root;

    // load Data
    int i,size = 0;
    Data *data;
    for (i = 0; i < pool_size; i++)
    {
        if(fread(&size, sizeof(int), 1, f) != 1)
        {
            goto FAIL;
        }
        if (size > 0)
        {
            data = (Data*)beans_safe_malloc(size + sizeof(Data*));
            if (fread(DATA_file_start(data), size, 1, f) != 1)
            {
                printf("load data: size %d fail\n", size);
                goto FAIL;
            }
            if (data->used != size)
            {
                printf("broken data: %d != %d\n", data->used, size);
                goto FAIL;
            }
            data->used = data->size = size + sizeof(Data*);
            data->next = NULL;
        }
        else if (size == 0)
        {
            data = NULL;
        }
        else
        {
            printf("unexpected size: %d\n", size);
            goto FAIL;
        }
        tree->root[i].data = data;
        pool_used++;
    }


    // load Codec
    if (fread(&size, sizeof(int), 1, f) != 1
            || size < 0 || size > (10<<20))
    {
        printf("bad codec size: %d\n", size);
        goto FAIL;
    }
    buf = (char*)beans_safe_malloc(size);
    if (fread(buf, size, 1, f) != 1)
    {
        printf("read codec failed");
        goto FAIL;
    }
    tree->dc = dc_new();
    if (dc_load(tree->dc, buf, size) != 0)
    {
        printf("load codec failed");
        goto FAIL;
    }
    free(buf);
    fclose(f);

    pthread_mutex_init(&tree->lock, NULL);

    return tree;

FAIL:
    if (tree->dc) 
        dc_destroy(tree->dc);
    if (buf) 
        free(buf);
    if (root)
    {
        for (i = 0; i < pool_used; i++)
        {
            if (root[i].data) free(root[i].data);
        }
        free(root);
    }
    free(tree);
    fclose(f);
    return NULL;
}

static int ht_save2(HTree *tree, FILE *f)
{
    size_t last_advise = 0;
    int fd = fileno(f);

    off_t pos = 0;
    if (fwrite(HTREE_VERSION, sizeof(HTREE_VERSION), 1, f) != 1 ||
            fwrite(&pos, sizeof(off_t), 1, f) != 1)
    {
        printf("write version failed");
        return -1;
    }

    int pool_size = g_index[tree->height];
    if (fwrite(&tree->height, sizeof(int), 1, f) != 1 ||
            fwrite(tree->root, sizeof(Node) * pool_size, 1, f) != 1 )
    {
        printf("write nodes failed");
        return -1;
    }

    int i, zero = 0;
    for (i = 0; i < pool_size; i++)
    {
        Data *data0= tree->root[i].data;
        if (data0)
        {
            Data new_data;
            init_data(&new_data, 0);
	        Data *data;
            for (data = data0; data != NULL; data = data->next)
            {
                new_data.count += data->count;
                new_data.used += data->used - DATA_HEAD_SIZE;
            }
            new_data.used -= sizeof(Data*);

            if (fwrite(&(new_data.used), sizeof(int), 1, f) != 1 || fwrite(DATA_file_start(&new_data), DATA_HEAD_SIZE - sizeof(Data*), 1, f) != 1)
            {
                printf("write data head failed");
                return -1;
            }

            for (data = data0; data != NULL; data = data->next)
            {
                if (data->count == 0)
                    continue;
                if ( fwrite(((char*)data) + DATA_HEAD_SIZE, data->used - DATA_HEAD_SIZE, 1, f) != 1)
                {
                    printf("write data failed");
                    return -1;
                }
            }
            file_dontneed(fd, ftello(f), &last_advise);
        }
        else
        {
            if (fwrite(&zero, sizeof(int), 1, f) != 1)
            {
                printf("write zero failed");
                return -1;
            }
        }
    }

    int s = dc_size(tree->dc);
    char *buf = (char*)beans_safe_malloc(s + sizeof(int));
    *(int*)buf = s;
    if (dc_dump(tree->dc, buf + sizeof(int), s) != s)
    {
        printf("dump Codec failed");
        free(buf);
        return -1;
    }
    if (fwrite(buf, s + sizeof(int), 1, f) != 1)
    {
        printf("write Codec failed");
        free(buf);
        return -1;
    }
    free(buf);

    pos = ftello(f);
    fseeko(f, sizeof(HTREE_VERSION), 0);
    if (fwrite(&pos, sizeof(off_t), 1, f) != 1)
    {
        printf("write size failed");
        return -1;
    }

    return 0;
}

int ht_save(HTree *tree, const char *path)
{
    if (!tree || !path) return -1;

    mgr_unlink(path);

    char tmp[MAX_PATH_LEN];
    safe_snprintf(tmp, MAX_PATH_LEN, "%s.tmp", path);

    FILE *f = fopen(tmp, "wb");
    if (f == NULL)
    {
        printf("open %s failed\n", tmp);
        return -1;
    }

    int buf_size = 1024*1024;
    char *buff = (char*)malloc(buf_size);
    memset( buff, '\0', buf_size);
    setvbuf(f, buff, _IOFBF, buf_size);

    uint64_t file_size = 0;
    struct timeval save_start, save_end;
    gettimeofday(&save_start, NULL);

    pthread_mutex_lock(&tree->lock);
    int ret = ht_save2(tree, f);
    pthread_mutex_unlock(&tree->lock);
    if (ret == 0)
    {
        fseeko(f, 0, SEEK_END);
        file_size = ftello(f);
    }
    fclose(f);
    free(buff);

    if (ret == 0)
    {
        gettimeofday(&save_end, NULL);
        float save_secs = (save_end.tv_sec - save_start.tv_sec) + (save_end.tv_usec - save_start.tv_usec) / 1e6;
        printf("save HTree to %s, size = %"PRIu64", in %f secs\n", path, file_size, save_secs);
        mgr_rename(tmp, path);
    }
    else
        mgr_unlink(tmp);
    return ret;
}

void ht_destroy(HTree *tree)
{
    if (!tree) return;

    pthread_mutex_lock(&tree->lock);

    dc_destroy(tree->dc);

    int i;
    int pool_size = g_index[tree->height];
    for(i = 0; i < pool_size; i++)
    {
        if (tree->root[i].data)
            free_data(tree->root + i);
    }
    free(tree->root);
    free(tree);
}

static inline uint32_t keyhash(const char *s, int len)
{
    return fnv1a(s, len);
}

bool check_key(const char *key, int len)
{
    if (!key) return false;
    if (len == 0 || len > MAX_KEY_LEN)
    {
        printf("bad key len=%d\n", len);
        return false;
    }
    if (key[0] <= ' ')
    {
        printf("bad key len=%d %x\n", len, key[0]);
        return false;
    }
    int k;
    for (k = 0; k < len; k++)
    {
        if (isspace(key[k]) || iscntrl(key[k]))
        {
            printf("bad key len=%d %s\n", len, key);
            return false;
        }
    }
    return true;
}

bool check_bucket(HTree *tree, const char* key, int len)
{
    uint32_t h = keyhash(key, len);
    if (tree->depth > 0 && h >> ((8-tree->depth) * 4) != (unsigned int)(tree->pos))
    {
        printf("key %s (#%x) should not in this tree (%d:%0x)\n", key, h >> ((8-tree->depth) * 4), tree->depth, tree->pos);
        return false;
    }

    return true;
}

void ht_add2(HTree *tree, const char *key, int len, uint32_t pos, uint16_t hash, int32_t ver)
{
    if (!check_bucket(tree, key, len)) return;
    Item *it = create_item(tree, key, len, pos, hash, ver);
    add_item(tree, tree->root, it, keyhash(key, len), true);
}

void ht_add(HTree *tree, const char *key, uint32_t pos, uint16_t hash, int32_t ver)
{
    pthread_mutex_lock(&tree->lock);
    ht_add2(tree, key, strlen(key), pos, hash, ver);
    pthread_mutex_unlock(&tree->lock);
}

void ht_remove2(HTree *tree, const char *key, int len)
{
    if (!check_bucket(tree, key, len)) return;
    Item *it = create_item(tree, key, len, 0, 0, 0);
    remove_item(tree, tree->root, it, keyhash(key, len));
}

void ht_remove(HTree *tree, const char *key)
{
    pthread_mutex_lock(&tree->lock);
    ht_remove2(tree, key, strlen(key));
    pthread_mutex_unlock(&tree->lock);
}

Item *ht_get2(HTree *tree, const char *key, int len)
{
    if (!check_bucket(tree, key, len)) return NULL;

    pthread_mutex_lock(&tree->lock);
    Item *it = create_item(tree, key, len, 0, 0, 0);
    Item *r = get_item_hash(tree, tree->root, it, keyhash(key, len));
    if (r != NULL)
    {
        Item *rr = (Item*)beans_safe_malloc(sizeof(Item) + len);
        memcpy(rr, r, sizeof(Item)); // safe
        memcpy(rr->key, key, len);  // safe
        rr->key[len] = 0; // c-str
        r = rr; // r is in node->Data block
    }
    pthread_mutex_unlock(&tree->lock);
    return r;
}

Item *ht_get(HTree *tree, const char *key)
{
    return ht_get2(tree, key, strlen(key));
}

uint32_t ht_get_hash(HTree *tree, const char *key, unsigned int *count)
{
    if (!tree || !key || key[0] != '@')
    {
        if(count) *count = 0;
        return 0;
    }

    uint32_t hash = 0;
    pthread_mutex_lock(&tree->lock);
    update_node(tree, tree->root);
    hash = get_node_hash(tree, tree->root, key+1, count);
    pthread_mutex_unlock(&tree->lock);
    return hash;
}

char *ht_list(HTree *tree, const char *dir, const char *prefix)
{
    if (!tree || !dir || strlen(dir) > 8) return NULL;
    if (prefix != NULL && strlen(prefix) == 0) prefix = NULL;

    pthread_mutex_lock(&tree->lock);
    char *r = list_dir(tree, tree->root, dir, prefix);
    pthread_mutex_unlock(&tree->lock);

    return r;
}

void ht_visit(HTree *tree, fun_visitor visitor, void *param)
{
    pthread_mutex_lock(&tree->lock);
    visit_node(tree, tree->root, visitor, param);
    pthread_mutex_unlock(&tree->lock);
}

void ht_visit2(HTree *tree, fun_visitor visitor, void *param)
{
    visit_node(tree, tree->root, visitor, param);
}

void ht_set_updating_bucket(HTree *tree, int bucket, HTree *updating_tree)
{
    printf("updating bucket %d for htree 0x%x\n", bucket, tree->pos);
    pthread_mutex_lock(&tree->lock);
    tree->updating_bucket = bucket;
    tree->updating_tree = updating_tree;
    pthread_mutex_unlock(&tree->lock);
}

Item *ht_get_withbuf(HTree *tree, const char *key, int len, char *buf, bool lock)
{
    if (!check_bucket(tree, key, len)) return NULL;

    Item *it = (Item*)buf;
    it->ksz = dc_encode(tree->dc, it->key, TREE_BUF_SIZE - (sizeof(Item) - ITEM_PADDING), key, len);

    if (lock)
        pthread_mutex_lock(&tree->lock);
    Item *r = get_item_hash(tree, tree->root, it, keyhash(key, len));
    if (r != NULL)
    {
        int l = ITEM_LENGTH(it);
        memcpy(it, r, l); // safe
        buf[l] = 0; // c-str
        r = it;
    }
    if (lock)
        pthread_mutex_unlock(&tree->lock);
    return r;
}


Item *ht_get_maybe_tmp(HTree *tree, const char *key, int *is_tmp, char *buf)
{
    *is_tmp = 0;
    Item *item = ht_get_withbuf(tree, key, strlen(key), buf, true);
    if (NULL != item)
    {
        uint32_t bucket = item->pos & 0xff;
        if (tree->updating_bucket == bucket)
        {
            pthread_mutex_lock(&tree->lock);
            if (tree->updating_bucket == bucket)
            {
                printf("get tmp for %s\n", key);
                *is_tmp = 1;
                item = ht_get_withbuf(tree->updating_tree, key, strlen(key), buf, false);
            }
            else
            {
                printf("get again for %s\n", key);
                item = ht_get_withbuf(tree, key, strlen(key), buf, false);
            }
            pthread_mutex_unlock(&tree->lock);
        }
    }
    return item;
}

