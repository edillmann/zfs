#include <assert.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <malloc.h>
#include <pthread.h>
#include <libzfs.h>

#define BUFFER_ENTRY_SIZE 131072
//#define  DEBUG 1
#undef DEBUG
#ifdef DEBUG
#define dprintf(args...) fprintf(args)
#else 
#define dprintf(args...) 
#endif

typedef struct lbuffer_entry
{
	uint32_t bsize;
	uint8_t last;
	uint8_t buf[BUFFER_ENTRY_SIZE];
} lbuffer_entry_t;

static void
lbuffer_input_cleanup(void *args)
{
	int err;
	dprintf(stderr, "\ninputThread: cleanup...\n");
	zfs_lbuffer_t *ctx = (zfs_lbuffer_t *) args;
	lbuffer_entry_t *lbuf = ctx->buffer[ctx->islot];
	lbuf->last = 1;
	err = sem_post(&ctx->buf2fd_sem);
	//close(ctx->input_fd);
	dprintf(stderr, "\ninputThread: finished...\n");
}

static void *
lbuffer_input_pthread(void *args)
{
	zfs_lbuffer_t *ctx = (zfs_lbuffer_t *) args;

	pthread_cleanup_push(lbuffer_input_cleanup, args);

	dprintf(stderr, "\ninputThread: starting...\n");
	for (;;) {
		int err;
		/* Wait for one or more buffer blocks to be free */
		err = sem_wait(&ctx->fd2buf_sem);
		assert(err == 0);
		/* handle async termination requests */
		if (ctx->terminate) {
			(void) pthread_cancel(ctx->writer_tid);
			pthread_exit(0);
			return NULL;
		}
		lbuffer_entry_t *lbuf = ctx->buffer[ctx->islot];
		lbuf->bsize = 0;
		do {
			int in;
			in = read(ctx->input_fd, lbuf->buf + lbuf->bsize, BUFFER_ENTRY_SIZE - lbuf->bsize);
			dprintf(stderr, "read %d %d\n", ctx->islot, in);
			if (-1 == in) {
				dprintf(stderr, "inputThread: error reading %s\n", strerror(errno));
				lbuf->last = 1;
				err = sem_post(&ctx->buf2fd_sem);
				dprintf(stderr, "sem_post %d\n", err);
				assert(err == 0);
				//close(ctx->input_fd);
				dprintf(stderr, "inputThread: exiting...\n");
				pthread_exit((void *) - 1);
			} else if (0 == in) {
				lbuf->last = 1;
				err = sem_post(&ctx->buf2fd_sem);
				dprintf(stderr, "sem_post %d\n", err);
				assert(err == 0);
				//close(ctx->input_fd);
				dprintf(stderr, "inputThread:last bloc exiting...\n");
				pthread_exit(0);
			}
			lbuf->bsize += in;
		} while (lbuf->bsize < BUFFER_ENTRY_SIZE);
		err = sem_post(&ctx->buf2fd_sem);
		dprintf(stderr, "sem_post %d\n", err);
		assert(err == 0);
		ctx->islot++;
		if (ctx->islot == ctx->entries)
			ctx->islot = 0;
	}
	pthread_cleanup_pop(0);
	return NULL;
}

static void
exit_output_pthread(zfs_lbuffer_t *ctx)
{
	dprintf(stderr, "outputThread: syncing...\n");
	while (0 != fsync(ctx->output_fd)) {
		if (errno == EINTR) {
			continue;
		} else if (errno == EINVAL) {
			//  output does not support syncing: ignore
			break;
		} else {
			// error syncing
			break;
		}
	}
	dprintf(stderr, "outputThread: finished - exiting...\n");
	pthread_exit(0);
}

static void *
lbuffer_output_pthread(void *args)
{
	int err;
	zfs_lbuffer_t *ctx = (zfs_lbuffer_t *) args;
	lbuffer_entry_t *lbuf;
	size_t rest;

	dprintf(stderr, "\noutputThread: starting...\n");
	for (;;) {
		err = sem_wait(&ctx->buf2fd_sem);
		lbuf = ctx->buffer[ctx->oslot];
		rest = lbuf->bsize;
		dprintf(stderr, "\nto_write: %d %d\n", ctx->oslot, lbuf->bsize);

		if (ctx->terminate) {
			dprintf(stderr, "\noutputThread: terminate...\n");
			//(void) pthread_cancel(ctx->reader_tid);
			/* TODO handle close return code */
			//close(ctx->output_fd);
			pthread_exit(0);
		}
		if (0 == lbuf->bsize && lbuf->last) {
			exit_output_pthread(ctx);
		}
		do {
			int num;
			{
				num = write(ctx->output_fd, lbuf->buf + lbuf->bsize - rest, rest);
				dprintf(stderr, "write %d %d\n", ctx->oslot, num);
			}
			if ((-1 == num) && ((errno == ENOMEM) || (errno == ENOSPC))) {
				continue;
			} else if (-1 == num) {
				dprintf(stderr, "outputThread: error writing at %s\n", strerror(errno));
				ctx->terminate = 1;
				err = sem_post(&ctx->fd2buf_sem);
				assert(err == 0);
				pthread_exit((void *) - 1);
			}
			rest -= num;
			dprintf(stderr, "rest %ld\n", rest);
		} while (rest > 0);
		dprintf(stderr, "last:%d\n", lbuf->last);
		if (lbuf->last) {
			exit_output_pthread(ctx);
		}
		err = sem_post(&ctx->fd2buf_sem);
		assert(err == 0);
		ctx->oslot++;
		if (ctx->entries == ctx->oslot)
			ctx->oslot = 0;
	}
}

zfs_lbuffer_t *
libzfs_lbuffer_alloc(uint64_t buffer_size, uint8_t compress)
{
	uint32_t entry;

	/* allocate buffering context */
	zfs_lbuffer_t *ctx = malloc(sizeof (zfs_lbuffer_t));
	if (ctx == NULL) {
		return NULL;
	}

	memset((void *) ctx, 0, sizeof (zfs_lbuffer_t));

/*	if (compress) {
		ctx->strm.zalloc = Z_NULL;
		ctx->strm.zfree = Z_NULL;
		ctx->strm.opaque = Z_NULL;
		if (deflateInit(&(ctx->strm), Z_BEST_SPEED) != Z_OK)
			goto mbuffer_nomem;
	}*/

	/* allocate buffer memory */
	ctx->entries = buffer_size / BUFFER_ENTRY_SIZE;
	dprintf(stderr, "entries %d\n", ctx->entries);
	ctx->buffer = malloc(sizeof (uint8_t *) * ctx->entries);
	if (ctx->buffer == NULL) {
		goto lbuffer_nomem;
	}
	for (entry = 0; entry < ctx->entries; entry++) {
		ctx->buffer[entry] = (lbuffer_entry_t *) malloc(sizeof (lbuffer_entry_t));
		if (ctx->buffer[entry] == NULL) {
			goto lbuffer_nomem;
		}
	}
	return ctx;

	/* free allocated memory */
lbuffer_nomem:
	if (ctx->buffer != NULL) {
		for (entry = 0; entry < ctx->entries; entry++) {
			if (ctx->buffer[entry] != NULL) {
				free(ctx->buffer[entry]);
				ctx->buffer[entry] = NULL;
			}
		}
		free(ctx->buffer);
	}
	free(ctx);
	return NULL;
}

static void
cancel_threads(zfs_lbuffer_t *ctx, int abort)
{
	dprintf(stderr, "free cancel reader\n");
	(void) pthread_cancel(ctx->reader_tid);
	pthread_join(ctx->reader_tid, 0);
	dprintf(stderr, "free joined reader\n");
	if (abort) {
		(void) pthread_cancel(ctx->writer_tid);
	}
	pthread_join(ctx->writer_tid, 0);
	dprintf(stderr, "free joined writer\n");
}

void
libzfs_lbuffer_free(zfs_lbuffer_t *ctx, int abort)
{
	uint32_t entry;

	if (ctx->buffer == NULL) {
		return;
	}

	cancel_threads(ctx, abort);
	
	for (entry = 0; entry < ctx->entries; entry++) {
		if (ctx->buffer[entry] != NULL) {
			free(ctx->buffer[entry]);
			ctx->buffer[entry] = NULL;
		}
	}
	free(ctx->buffer);

	free(ctx);
}

int
libzfs_lbuffer_input_fd(int in_fd, zfs_lbuffer_t *ctx)
{
	int err;
	int pipefd[2];

	/*
	 * allocate pipe
	 * pipefd[0] is for reading
	 * pipefd[1] IS for writing
	 */
	if ((err = pipe(pipefd))) {
		return err;
	}

	ctx->input_fd = in_fd;
	ctx->output_fd = pipefd[1];

	err = sem_init(&ctx->buf2fd_sem, 0, 0);
	err = sem_init(&ctx->fd2buf_sem, 0, ctx->entries);

	err = pthread_create(&ctx->writer_tid, 0, &lbuffer_output_pthread, ctx);
	err = pthread_create(&ctx->reader_tid, 0, &lbuffer_input_pthread, ctx);
	return pipefd[0];
}

int
libzfs_lbuffer_output_fd(int out_fd, zfs_lbuffer_t *ctx)
{

	int err;
	int pipefd[2];

	/*
	 * allocate pipe
	 * pipefd[0] is for reading
	 * pipefd[1] IS for writing
	 */
	if ((err = pipe(pipefd))) {
		return err;
	}

	ctx->input_fd = pipefd[0];
	ctx->output_fd = out_fd;

	err = sem_init(&ctx->buf2fd_sem, 0, 0);
	err = sem_init(&ctx->fd2buf_sem, 0, ctx->entries);

	err = pthread_create(&ctx->writer_tid, 0, &lbuffer_output_pthread, ctx);
	err = pthread_create(&ctx->reader_tid, 0, &lbuffer_input_pthread, ctx);
	return pipefd[1];
}
