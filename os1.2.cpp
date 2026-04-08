#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <aio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <signal.h>
#include <pthread.h>

#include <iostream>
#include <string>
#include <atomic>

// ── tunables ──────────────────────────────────────────────────────────────
static const int CLUSTER   = 4096;
static const int MAX_SLOTS = 64;

// ── per-operation descriptor ──────────────────────────────────────────────
struct aio_operation {
    struct aiocb  aio;
    char         *buffer;
    int           write_operation; // 0 = read, 1 = write
    void         *next_operation;  // reserved
    int           slot;
    off_t         offset;
    ssize_t       bytes;           // result of aio_return (called exactly once)
};

// ── global copy state (reset before each copy) ────────────────────────────
static int   g_fd_src   = -1;
static int   g_fd_dst   = -1;
static off_t g_filesize = 0;

static int g_block_size = CLUSTER;
static int g_n_slots    = 1;

static long g_total_blocks = 0;

static std::atomic<long> g_next_block(0);  // next block index to read
static std::atomic<long> g_done_writes(0); // number of completed writes

// mutex+condvar used to signal main thread when all writes are done
static pthread_mutex_t g_mtx     = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cond    = PTHREAD_COND_INITIALIZER;
static volatile int    g_finished = 0;

static aio_operation *g_rops = nullptr;
static aio_operation *g_wops = nullptr;

// ── forward declaration ───────────────────────────────────────────────────
static void launch_read(int slot, long block_idx);

// ── completion callback (called by kernel via SIGEV_THREAD) ───────────────
void aio_completion_handler(sigval_t sv)
{
    aio_operation *op = (aio_operation *)sv.sival_ptr;
    int slot = op->slot;

    op->bytes = aio_return(&op->aio); // must be called exactly once

    if (op->write_operation) {
        // write done: signal if last block, otherwise kick next read
        long done = ++g_done_writes;
        if (done >= g_total_blocks) {
            pthread_mutex_lock(&g_mtx);
            g_finished = 1;
            pthread_cond_signal(&g_cond);
            pthread_mutex_unlock(&g_mtx);
            return;
        }
        long next = g_next_block.fetch_add(1);
        if (next < g_total_blocks)
            launch_read(slot, next);
    } else {
        // read done: issue write for the same slot using the same buffer
        aio_operation *wop = &g_wops[slot];
        memset(&wop->aio, 0, sizeof(struct aiocb));
        wop->aio.aio_fildes  = g_fd_dst;
        wop->aio.aio_offset  = op->offset;
        wop->aio.aio_buf     = op->buffer;
        wop->aio.aio_nbytes  = (size_t)op->bytes;
        wop->aio.aio_sigevent.sigev_notify            = SIGEV_THREAD;
        wop->aio.aio_sigevent.sigev_notify_function   = aio_completion_handler;
        wop->aio.aio_sigevent.sigev_notify_attributes = nullptr;
        wop->aio.aio_sigevent.sigev_value.sival_ptr   = wop;
        wop->write_operation = 1;
        wop->slot   = slot;
        wop->offset = op->offset;
        wop->bytes  = 0;

        if (aio_write(&wop->aio) < 0)
            perror("aio_write");
    }
}

// ── issue async read for one block ────────────────────────────────────────
static void launch_read(int slot, long block_idx)
{
    off_t  offset  = (off_t)block_idx * g_block_size;
    size_t to_read = (size_t)g_block_size;
    if (offset + (off_t)to_read > g_filesize)
        to_read = (size_t)(g_filesize - offset);

    aio_operation *rop = &g_rops[slot];
    memset(&rop->aio, 0, sizeof(struct aiocb));
    rop->aio.aio_fildes  = g_fd_src;
    rop->aio.aio_offset  = offset;
    rop->aio.aio_buf     = rop->buffer;
    rop->aio.aio_nbytes  = to_read;
    rop->aio.aio_sigevent.sigev_notify            = SIGEV_THREAD;
    rop->aio.aio_sigevent.sigev_notify_function   = aio_completion_handler;
    rop->aio.aio_sigevent.sigev_notify_attributes = nullptr;
    rop->aio.aio_sigevent.sigev_value.sival_ptr   = rop;
    rop->write_operation = 0;
    rop->slot   = slot;
    rop->offset = offset;
    rop->bytes  = 0;

    if (aio_read(&rop->aio) < 0)
        perror("aio_read");
}

// ── copy src→dst, return elapsed ms (-1 on error) ─────────────────────────
static int64_t do_copy(const char *src, const char *dst)
{
    g_fd_src = open(src, O_RDONLY | O_NONBLOCK);
    if (g_fd_src < 0) { perror("open src"); return -1; }

    struct stat st;
    fstat(g_fd_src, &st);
    g_filesize = st.st_size;
    if (g_filesize == 0) { fprintf(stderr, "source file is empty\n"); close(g_fd_src); return -1; }

    g_fd_dst = open(dst, O_CREAT | O_WRONLY | O_TRUNC | O_NONBLOCK, 0666);
    if (g_fd_dst < 0) { perror("open dst"); close(g_fd_src); return -1; }

    g_total_blocks = (g_filesize + g_block_size - 1) / g_block_size;
    g_next_block.store(0);
    g_done_writes.store(0);
    g_finished = 0;

    g_rops = new aio_operation[g_n_slots]();
    g_wops = new aio_operation[g_n_slots]();
    for (int i = 0; i < g_n_slots; i++) {
        g_rops[i].buffer = new char[g_block_size];
        g_rops[i].slot = i;
        g_wops[i].slot = i;
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    // start initial reads, one per slot (up to total_blocks)
    int first = (g_n_slots < (int)g_total_blocks) ? g_n_slots : (int)g_total_blocks;
    for (int i = 0; i < first; i++)
        launch_read(i, g_next_block.fetch_add(1));

    // block main thread until all writes finish
    pthread_mutex_lock(&g_mtx);
    while (!g_finished)
        pthread_cond_wait(&g_cond, &g_mtx);
    pthread_mutex_unlock(&g_mtx);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    int64_t ms = (int64_t)(t1.tv_sec  - t0.tv_sec)  * 1000
               + (int64_t)(t1.tv_nsec - t0.tv_nsec) / 1000000;

    close(g_fd_src);
    close(g_fd_dst);
    for (int i = 0; i < g_n_slots; i++) delete[] g_rops[i].buffer;
    delete[] g_rops;
    delete[] g_wops;
    g_rops = g_wops = nullptr;

    return ms;
}

// ── helpers ───────────────────────────────────────────────────────────────
static double file_mb(const char *path)
{
    struct stat st; stat(path, &st);
    return (double)st.st_size / 1048576.0;
}

static void print_menu()
{
    printf("\n=========================================\n");
    printf("  AIO File Copy  |  block=%d B  slots=%d\n", g_block_size, g_n_slots);
    printf("=========================================\n");
    printf("  1. Copy file\n");
    printf("  2. Set block size (cluster multiples)\n");
    printf("  3. Set parallel slot count\n");
    printf("  4. Experiment: speed vs block size\n");
    printf("  5. Experiment: speed vs slot count\n");
    printf("  0. Exit\n");
    printf("Choice: ");
}

// ── menu actions ──────────────────────────────────────────────────────────
static void menu_copy()
{
    std::string src, dst;
    std::cout << "Source file : "; std::cin >> src;
    std::cout << "Dest file   : "; std::cin >> dst;
    int64_t ms = do_copy(src.c_str(), dst.c_str());
    if (ms < 0) return;
    double mb = file_mb(src.c_str());
    printf("Done: %.2f MB in %" PRId64 " ms  =>  %.2f MB/s\n", mb, ms, mb / (ms / 1000.0));
}

static void menu_set_block()
{
    int x; printf("Cluster multiplier: "); scanf("%d", &x);
    if (x < 1) x = 1;
    g_block_size = x * CLUSTER;
    printf("Block size = %d bytes\n", g_block_size);
}

static void menu_set_slots()
{
    printf("Slot count (1-%d): ", MAX_SLOTS); scanf("%d", &g_n_slots);
    if (g_n_slots < 1) g_n_slots = 1;
    if (g_n_slots > MAX_SLOTS) g_n_slots = MAX_SLOTS;
}

static void menu_exp_block()
{
    std::string src, dst;
    std::cout << "Source: "; std::cin >> src;
    std::cout << "Dest  : "; std::cin >> dst;

    int saved_b = g_block_size, saved_s = g_n_slots;
    g_n_slots = 1;

    int mults[] = {1, 2, 4, 8, 16, 32, 64, 128};
    int nm = (int)(sizeof(mults)/sizeof(mults[0]));
    double fmb = file_mb(src.c_str());

    printf("\n%-18s  %-10s  %-12s\n", "Block size (B)", "Time (ms)", "Speed (MB/s)");
    printf("%-18s  %-10s  %-12s\n", "------------------","----------","------------");

    int best_m = 1; double best_spd = 0;
    for (int i = 0; i < nm; i++) {
        g_block_size = mults[i] * CLUSTER;
        int64_t ms = do_copy(src.c_str(), dst.c_str());
        if (ms <= 0) { printf("%-18d  ERROR\n", g_block_size); continue; }
        double spd = fmb / (ms / 1000.0);
        printf("%-18d  %-10" PRId64 "  %-12.2f\n", g_block_size, ms, spd);
        if (spd > best_spd) { best_spd = spd; best_m = mults[i]; }
    }
    printf("\nBest block size: %d B (%dx cluster)  =>  %.2f MB/s\n",
           best_m * CLUSTER, best_m, best_spd);

    g_block_size = saved_b; g_n_slots = saved_s;
}

static void menu_exp_slots()
{
    std::string src, dst;
    std::cout << "Source: "; std::cin >> src;
    std::cout << "Dest  : "; std::cin >> dst;
    int mult; printf("Block cluster multiplier: "); scanf("%d", &mult);
    g_block_size = (mult < 1 ? 1 : mult) * CLUSTER;

    int saved_s = g_n_slots;
    int ops[] = {1, 2, 4, 8, 12, 16};
    int nl = (int)(sizeof(ops)/sizeof(ops[0]));
    double fmb = file_mb(src.c_str());

    printf("\n%-10s  %-10s  %-12s\n", "Slots", "Time (ms)", "Speed (MB/s)");
    printf("%-10s  %-10s  %-12s\n", "----------","----------","------------");

    int best_n = 1; double best_spd = 0;
    for (int i = 0; i < nl; i++) {
        g_n_slots = ops[i];
        int64_t ms = do_copy(src.c_str(), dst.c_str());
        if (ms <= 0) { printf("%-10d  ERROR\n", g_n_slots); continue; }
        double spd = fmb / (ms / 1000.0);
        printf("%-10d  %-10" PRId64 "  %-12.2f\n", g_n_slots, ms, spd);
        if (spd > best_spd) { best_spd = spd; best_n = g_n_slots; }
    }
    printf("\nBest slot count: %d  =>  %.2f MB/s\n", best_n, best_spd);

    g_n_slots = saved_s;
}

// ── main ──────────────────────────────────────────────────────────────────
int main()
{
    int ch;
    while (true) {
        print_menu();
        if (scanf("%d", &ch) != 1) break;
        switch (ch) {
            case 1: menu_copy();      break;
            case 2: menu_set_block(); break;
            case 3: menu_set_slots(); break;
            case 4: menu_exp_block(); break;
            case 5: menu_exp_slots(); break;
            case 0: puts("Bye."); return 0;
            default: puts("Unknown option.");
        }
    }
}