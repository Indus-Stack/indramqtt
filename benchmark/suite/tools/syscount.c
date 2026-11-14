// Syscall counter for the broker process. A diagnosis tool, not part of the product.
//
// Why: the fields syscr and syscw in /proc/<pid>/io count only read() and
// write() style calls on a file. The broker sends and receives on sockets with
// send() and recv(), and the kernel does not count those two calls there.
// This library is loaded with LD_PRELOAD. It counts the calls of the C library
// that the Rust runtime uses, and keeps the counters in a shared file that the
// driver reads while the broker runs.
//
// Build: gcc -O2 -shared -fPIC -o libsyscount.so syscount.c -ldl
// Use:   SYSCOUNT_FILE=/run/syscount.bin LD_PRELOAD=/path/libsyscount.so indramqtt ...
//
// The file holds STRIPES blocks of NC unsigned 64-bit counters. A thread uses
// the block of its own thread id, so the threads do not share a cache line.

#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#define NC 16
#define STRIPES 64

enum {
    C_SEND, C_SEND_BYTES, C_RECV, C_RECV_BYTES, C_READ, C_READ_BYTES,
    C_WRITE, C_WRITE_BYTES, C_EPOLL_WAIT, C_EPOLL_EVENTS, C_EPOLL_CTL,
    C_FUTEX_WAIT, C_FUTEX_WAKE, C_VEC_OUT, C_VEC_IN, C_SPARE
};

static uint64_t *tab;
static __thread int stripe = -1;

static ssize_t (*r_send)(int, const void *, size_t, int);
static ssize_t (*r_sendto)(int, const void *, size_t, int, const struct sockaddr *, socklen_t);
static ssize_t (*r_sendmsg)(int, const struct msghdr *, int);
static ssize_t (*r_recv)(int, void *, size_t, int);
static ssize_t (*r_recvfrom)(int, void *, size_t, int, struct sockaddr *, socklen_t *);
static ssize_t (*r_recvmsg)(int, struct msghdr *, int);
static ssize_t (*r_read)(int, void *, size_t);
static ssize_t (*r_write)(int, const void *, size_t);
static ssize_t (*r_writev)(int, const struct iovec *, int);
static ssize_t (*r_readv)(int, const struct iovec *, int);
static int (*r_epoll_wait)(int, struct epoll_event *, int, int);
static int (*r_epoll_pwait)(int, struct epoll_event *, int, int, const sigset_t *);
static int (*r_epoll_ctl)(int, int, int, struct epoll_event *);
static long (*r_syscall)(long, ...);

static inline uint64_t *blk(void) {
    if (!tab) return NULL;
    if (stripe < 0) stripe = (int)(r_syscall(SYS_gettid) & (STRIPES - 1));
    return tab + (size_t)stripe * NC;
}
static inline void add(int c, uint64_t v) {
    uint64_t *b = blk();
    if (b) __atomic_fetch_add(&b[c], v, __ATOMIC_RELAXED);
}

__attribute__((constructor)) static void init(void) {
    r_send = dlsym(RTLD_NEXT, "send");
    r_sendto = dlsym(RTLD_NEXT, "sendto");
    r_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
    r_recv = dlsym(RTLD_NEXT, "recv");
    r_recvfrom = dlsym(RTLD_NEXT, "recvfrom");
    r_recvmsg = dlsym(RTLD_NEXT, "recvmsg");
    r_read = dlsym(RTLD_NEXT, "read");
    r_write = dlsym(RTLD_NEXT, "write");
    r_writev = dlsym(RTLD_NEXT, "writev");
    r_readv = dlsym(RTLD_NEXT, "readv");
    r_epoll_wait = dlsym(RTLD_NEXT, "epoll_wait");
    r_epoll_pwait = dlsym(RTLD_NEXT, "epoll_pwait");
    r_epoll_ctl = dlsym(RTLD_NEXT, "epoll_ctl");
    r_syscall = dlsym(RTLD_NEXT, "syscall");
    const char *path = getenv("SYSCOUNT_FILE");
    if (!path) return;
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) return;
    size_t sz = (size_t)STRIPES * NC * 8;
    if (ftruncate(fd, (off_t)sz) != 0) return;
    void *m = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (m != MAP_FAILED) { memset(m, 0, sz); tab = (uint64_t *)m; }
}

ssize_t send(int fd, const void *b, size_t n, int f) {
    ssize_t r = r_send(fd, b, n, f);
    add(C_SEND, 1); if (r > 0) add(C_SEND_BYTES, (uint64_t)r);
    return r;
}
ssize_t sendto(int fd, const void *b, size_t n, int f, const struct sockaddr *a, socklen_t l) {
    ssize_t r = r_sendto(fd, b, n, f, a, l);
    add(C_SEND, 1); if (r > 0) add(C_SEND_BYTES, (uint64_t)r);
    return r;
}
ssize_t sendmsg(int fd, const struct msghdr *m, int f) {
    ssize_t r = r_sendmsg(fd, m, f);
    add(C_VEC_OUT, 1); if (r > 0) add(C_SEND_BYTES, (uint64_t)r);
    return r;
}
ssize_t recv(int fd, void *b, size_t n, int f) {
    ssize_t r = r_recv(fd, b, n, f);
    add(C_RECV, 1); if (r > 0) add(C_RECV_BYTES, (uint64_t)r);
    return r;
}
ssize_t recvfrom(int fd, void *b, size_t n, int f, struct sockaddr *a, socklen_t *l) {
    ssize_t r = r_recvfrom(fd, b, n, f, a, l);
    add(C_RECV, 1); if (r > 0) add(C_RECV_BYTES, (uint64_t)r);
    return r;
}
ssize_t recvmsg(int fd, struct msghdr *m, int f) {
    ssize_t r = r_recvmsg(fd, m, f);
    add(C_VEC_IN, 1); if (r > 0) add(C_RECV_BYTES, (uint64_t)r);
    return r;
}
ssize_t read(int fd, void *b, size_t n) {
    ssize_t r = r_read(fd, b, n);
    add(C_READ, 1); if (r > 0) add(C_READ_BYTES, (uint64_t)r);
    return r;
}
ssize_t write(int fd, const void *b, size_t n) {
    ssize_t r = r_write(fd, b, n);
    add(C_WRITE, 1); if (r > 0) add(C_WRITE_BYTES, (uint64_t)r);
    return r;
}
ssize_t writev(int fd, const struct iovec *v, int c) {
    ssize_t r = r_writev(fd, v, c);
    add(C_VEC_OUT, 1); if (r > 0) add(C_WRITE_BYTES, (uint64_t)r);
    return r;
}
ssize_t readv(int fd, const struct iovec *v, int c) {
    ssize_t r = r_readv(fd, v, c);
    add(C_VEC_IN, 1); if (r > 0) add(C_READ_BYTES, (uint64_t)r);
    return r;
}
int epoll_wait(int ep, struct epoll_event *e, int max, int t) {
    int r = r_epoll_wait(ep, e, max, t);
    add(C_EPOLL_WAIT, 1); if (r > 0) add(C_EPOLL_EVENTS, (uint64_t)r);
    return r;
}
int epoll_pwait(int ep, struct epoll_event *e, int max, int t, const sigset_t *s) {
    int r = r_epoll_pwait(ep, e, max, t, s);
    add(C_EPOLL_WAIT, 1); if (r > 0) add(C_EPOLL_EVENTS, (uint64_t)r);
    return r;
}
int epoll_ctl(int ep, int op, int fd, struct epoll_event *e) {
    add(C_EPOLL_CTL, 1);
    return r_epoll_ctl(ep, op, fd, e);
}
long syscall(long n, ...) {
    va_list ap;
    va_start(ap, n);
    long a1 = va_arg(ap, long), a2 = va_arg(ap, long), a3 = va_arg(ap, long);
    long a4 = va_arg(ap, long), a5 = va_arg(ap, long), a6 = va_arg(ap, long);
    va_end(ap);
    if (n == SYS_futex) {
        int op = (int)a2 & 127;
        if (op == 0 || op == 9) add(C_FUTEX_WAIT, 1);
        else if (op == 1 || op == 10) add(C_FUTEX_WAKE, 1);
    }
    return r_syscall(n, a1, a2, a3, a4, a5, a6);
}
