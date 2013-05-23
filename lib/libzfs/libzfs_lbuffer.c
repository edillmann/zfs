#include <assert.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <malloc.h>
#include <pthread.h>
#include <libzfs.h>
#include <poll.h>
#include <fcntl.h>
#include "lz4.h"

#define HDRSIZE sizeof(uint32_t)
#define BUFFER_ENTRY_SIZE (128*1028+HDRSIZE)
#define MAGIC 0x7a53465a
#define NOT_COMPRESSED (1 << 30)

#if GCC_VERSION >= 403
#define swap32 __builtin_bswap32
#else

static inline unsigned int
swap32(unsigned int x)
{
	return ((x << 24) & 0xff000000) |
		((x << 8) & 0x00ff0000) |
		((x >> 8) & 0x0000ff00) |
		((x >> 24) & 0x000000ff);
}
#endif

static const int one = 1;
#define CPU_LITTLE_ENDIAN   (*(char*)(&one))
#define CPU_BIG_ENDIAN      (!CPU_LITTLE_ENDIAN)
#define LITTLE_ENDIAN_32(i) (CPU_LITTLE_ENDIAN?(i):swap32(i))

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

static void *
lbuffer_input_pthread(void *args)
{
	zfs_lbuffer_t *ctx = (zfs_lbuffer_t *) args;
	struct pollfd pfds[1];
	uint32_t islot = 0;

	pfds[0].fd = ctx->input_fd;
	pfds[0].events = POLLIN;

	dprintf(stderr, "INPUT: starting...\n");
	for (;;) {
		int err;
		/* Wait for one or more buffer blocks to be free */
		dprintf(stderr, "INPUT wait wrTibuf %d\n", islot);
		err = sem_wait(&ctx->wrTibuf);
		assert(err == 0);
		if (ctx->terminate) {
			dprintf(stderr, "INPUT terminate\n");
			pthread_exit(0);
			return NULL;
		}

		lbuffer_entry_t *ibuf = ctx->i_buffer[islot];
		ibuf->bsize = 0;
		do {
			if (poll(pfds, 1, 500) == 1) {
				int in = read(ctx->input_fd, ibuf->buf + ibuf->bsize, BUFFER_ENTRY_SIZE - ibuf->bsize);
				if (-1 == in) {
					ibuf->last = 1;
					err = sem_post(&ctx->rdFibuf);
					assert(err == 0);
					dprintf(stderr, "INPUT: exiting...\n");
					pthread_exit((void *) - 1);
				} else if (0 == in) {
					ibuf->last = 1;
					err = sem_post(&ctx->rdFibuf);
					assert(err == 0);
					pthread_exit(0);
				}
				ibuf->bsize += in;
			} else if (ctx->finished) {
				ibuf->last = 1;
				dprintf(stderr, "INPUT post LAST rdFibuf slot:%d size:%d\n", islot, ibuf->bsize);
				err = sem_post(&ctx->rdFibuf);
				assert(err == 0);
				dprintf(stderr, "INPUT:last bloc exiting...\n");
				pthread_exit(0);
			}
		} while (ibuf->bsize < BUFFER_ENTRY_SIZE);
		err = sem_post(&ctx->rdFibuf);
		assert(err == 0);
		islot++;
		if (islot == ctx->i_entries)
			islot = 0;
	}
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
	close(ctx->output_fd);
	close(ctx->input_fd);
	dprintf(stderr, "outputThread: finished - exiting...\n");
	pthread_exit(0);
}

static void *
lbuffer_forward_pthread(void *args)
{
	int err;
	zfs_lbuffer_t *ctx = (zfs_lbuffer_t *) args;
	lbuffer_entry_t *ibuf, *obuf;
	uint32_t islot = 0, oslot = 0;
	uint8_t last;

	dprintf(stderr, "FORWARD: starting...\n");
	for (;;) {
		err = sem_wait(&ctx->rdFibuf);
		err = sem_wait(&ctx->wrTobuf);
		ibuf = ctx->i_buffer[islot];
		obuf = ctx->o_buffer[oslot];
		//dprintf(stderr, "\nforward buffer: %d %d\n", islot, ibuf->bsize);
		if (ctx->terminate) {
			dprintf(stderr, "FORWARD: terminate...\n");
			pthread_exit(0);
		}
		obuf->bsize = ibuf->bsize;
		last = obuf->last = ibuf->last;
		memcpy(obuf->buf, ibuf->buf, ibuf->bsize);
		err = sem_post(&ctx->rdFobuf);
		err = sem_post(&ctx->wrTibuf);
		if (last) {
			dprintf(stderr, "FORWARD: done...\n");
			pthread_exit(0);
		}
		islot++;
		if (ctx->i_entries == islot)
			islot = 0;
		oslot++;
		if (ctx->o_entries == oslot)
			oslot = 0;
	}
}

static void *
lbuffer_uncompress_pthread(void *args)
{
	uint8_t first = 1;
	uint8_t last = 0;
	int err;
	zfs_lbuffer_t *ctx = (zfs_lbuffer_t *) args;
	lbuffer_entry_t *obuf;
	lbuffer_entry_t *ibuf;
	uint32_t zislot = 0;
	uint32_t zoslot = 0;

	uint8_t *workmem;
	uint32_t ioffset = 0;
	int32_t uncomp = 0;
	uint32_t wmemsize = 0;
	uint8_t copy = 0;

	int comp = 0;
	dprintf(stderr, "LZ4: starting...\n");

	err = sem_wait(&ctx->wrTobuf);
	obuf = ctx->o_buffer[zoslot];
	for (;;) {
		//dprintf(stderr, "\nrd:%d wms:%d comp:%d zi:%d zo:%d\n", rd, wms, comp, zislot, zoslot);
		ioffset = 0;
		dprintf(stderr, "LZ4 wait rdFibuf %d\n", zislot);
		err = sem_wait(&ctx->rdFibuf);
		dprintf(stderr, "LZ4+rdFibuf %d\n", zislot);
		ibuf = ctx->i_buffer[zislot];
		last = ibuf->last;
		// compressed stream detection
		if (first) {
			if ((ibuf->bsize >= 2*HDRSIZE) && LITTLE_ENDIAN_32(*(uint32_t *) ibuf->buf) == MAGIC) {
				dprintf(stderr, "LZ4: compressed stream\n");
				ioffset += HDRSIZE;
				workmem = malloc(2 * BUFFER_ENTRY_SIZE);
				if (workmem == NULL) {
					dprintf(stderr, "LZ4: ENOMEM\n");
					ctx->terminate = 1;
				}
				ctx->compress = 1;
			}
			first = 0;
		}
		if (ctx->terminate) {
			dprintf(stderr, "LZ4: terminate...\n");
			pthread_exit(0);
		}

		if (ctx->compress == 0) {
			//dprintf(stderr, "\nforward buffer: %d %d\n", islot, ibuf->bsize);
			if (ctx->terminate) {
				dprintf(stderr, "FORWARD: terminate...\n");
				pthread_exit(0);
			}
			obuf = ctx->o_buffer[zoslot];
			obuf->bsize = ibuf->bsize;
			last = obuf->last = ibuf->last;
			memcpy(obuf->buf, ibuf->buf, ibuf->bsize);
			err = sem_post(&ctx->rdFobuf);
			err = sem_post(&ctx->wrTibuf);
			if (last) {
				dprintf(stderr, "FORWARD: done...\n");
				pthread_exit(0);
			}
			zislot++;
			if (ctx->i_entries == zislot)
				zislot = 0;
			err = sem_wait(&ctx->wrTobuf);
			zoslot++;
			if (ctx->o_entries == zoslot)
				zoslot = 0;
			continue;
		}

		// copy buffer to workmem
		dprintf(stderr, "LZ4 memcpy @%d rd:%d sz:%d\n", wmemsize, ioffset, ibuf->bsize - ioffset);
		memcpy(workmem + wmemsize, ibuf->buf + ioffset, ibuf->bsize - ioffset);
		wmemsize += ibuf->bsize - ioffset;
		dprintf(stderr, "LZ4 post wrTibuf %d\n", zoslot);
		err = sem_post(&ctx->wrTibuf);
		// read chunk size
		comp = LITTLE_ENDIAN_32(*(uint32_t *) workmem);
		copy = (comp & NOT_COMPRESSED) ? 1 : 0;
		comp &= (~NOT_COMPRESSED);
		dprintf(stderr, "LZ4 read comp:%d wms:%d copy:%d\n", comp, wmemsize, copy);
		while (comp <= wmemsize - HDRSIZE) {
			ioffset = HDRSIZE;
			// uncompress chunk
			if (copy) {
				memcpy(obuf->buf, workmem + ioffset, comp);
				uncomp = comp;
			} else {
				uncomp = LZ4_uncompress_unknownOutputSize((const char *) workmem + ioffset, (char *) obuf->buf, comp, LZ4_compressBound(BUFFER_ENTRY_SIZE));
			}
			dprintf(stderr, "LZ4 read chunk:%d uncomp:%d\n", comp, uncomp);
			if (uncomp <= 0) {
				ctx->terminate = 1;
				dprintf(stderr, "\nLZ4 ## uncompress error: terminate...\n");
				pthread_exit(0);
			}
			ioffset += comp;
			wmemsize -= ioffset;
			obuf->bsize = uncomp;
			obuf->last = ((wmemsize == 0) && last) ? 1 : 0;
			err = sem_post(&ctx->rdFobuf);
			if ((wmemsize == 0) && last) {
				dprintf(stderr, "\nLZ4 ## uncompress done: terminate...\n");
				pthread_exit(0);
			}
			err = sem_wait(&ctx->wrTobuf);
			zoslot++;
			if (ctx->o_entries == zoslot)
				zoslot = 0;
			obuf = ctx->o_buffer[zoslot];
			if (wmemsize) {
				memmove(workmem, workmem + ioffset, wmemsize);
				if (wmemsize > 3) {
					// read chunk size
					comp = LITTLE_ENDIAN_32(*(uint32_t *) workmem);
					copy = (comp & NOT_COMPRESSED) ? 1 : 0;
					comp &= (~NOT_COMPRESSED);
				} else {
					// bail out
					comp = -4;
				}
			} else {
				// bail out
				comp = -4;
				if (last) {
					dprintf(stderr, "LZ4: done...\n");
					pthread_exit(0);
				}
			}
		}

		zislot++;
		if (ctx->i_entries == zislot)
			zislot = 0;
		//ibuf = ctx->i_buffer[zislot];
	}
}

static void *
lbuffer_compress_pthread(void *args)
{
	int err;
	zfs_lbuffer_t *ctx = (zfs_lbuffer_t *) args;
	lbuffer_entry_t *obuf;
	lbuffer_entry_t *ibuf;
	uint32_t zislot = 0;
	uint32_t zoslot = 0;
	uint32_t wr = 0;
	uint32_t rd = 0;
	uint32_t comp = 0;

	uint8_t *workmem = malloc(LZ4_compressBound(BUFFER_ENTRY_SIZE));
	if (workmem == NULL) {
		ctx->terminate=1;
		pthread_exit(0);
	}
	dprintf(stderr, "CCLZ4: starting...\n");
	rd = 0;

	err = sem_wait(&ctx->wrTobuf);
	obuf = ctx->o_buffer[zoslot];
	ibuf = ctx->i_buffer[zislot];
	// write MAGIC
	*(uint32_t *) obuf->buf = LITTLE_ENDIAN_32(MAGIC);

	wr = HDRSIZE;
	for (;;) {
		dprintf(stderr, "\nLZ4 rd:%d wr:%d comp:%d zi:%d zo:%d\n", rd, wr, comp, zislot, zoslot);

		if (rd == 0) {
			dprintf(stderr, "CCLZ4 wait rdFibuf %d\n", zislot);
			err = sem_wait(&ctx->rdFibuf);
			dprintf(stderr, "CCLZ4 got wait rdFibuf %d\n", zislot);
			ibuf = ctx->i_buffer[zislot];

			//dprintf(stderr, "\nto_compress: %d %d\n", zislot, ibuf->bsize);
			if (ctx->terminate) {
				dprintf(stderr, "CCLZ4: terminate...\n");
				pthread_exit(0);
			}
			comp = LZ4_compress_limitedOutput((const char *) ibuf->buf, (char *) workmem, ibuf->bsize,BUFFER_ENTRY_SIZE);

			if (comp == 0 || comp >= ibuf->bsize) {
				comp = ibuf->bsize | NOT_COMPRESSED;
				memcpy(workmem,ibuf->buf,ibuf->bsize);
			}

			dprintf(stderr, "CCLZ4 post wrTibuf %d\n", zislot);
			err = sem_post(&ctx->wrTibuf);
			dprintf(stderr, "CCLZ4 %d compressed to %d\n", ibuf->bsize, comp);
			// write original size
			*(uint32_t *) (obuf->buf + wr) = LITTLE_ENDIAN_32(comp);
			comp &= (~NOT_COMPRESSED);
			wr += HDRSIZE;
			dprintf(stderr, "x[SZ]\n");

			zislot++;
			if (ctx->i_entries == zislot)
				zislot = 0;
		}

		uint32_t cpy = (BUFFER_ENTRY_SIZE - wr > comp) ? comp : BUFFER_ENTRY_SIZE - wr;
		dprintf(stderr, "CCLZ4 memcpy t:%d wm:%d n:%d\n", wr, rd, cpy);
		memcpy(obuf->buf + wr, workmem + rd, cpy);
		dprintf(stderr, "x[%d]\n", cpy);

		wr += cpy;

		if (cpy < comp) {
			rd += cpy;
			comp -= cpy;
		} else {
			rd = 0;
		}

		if ((wr > BUFFER_ENTRY_SIZE - 4*HDRSIZE) || (ibuf -> last)) {
			obuf->bsize = wr;
			obuf->last = ((rd == 0) && ibuf->last) ? 1 : 0;
			dprintf(stderr, "lz4 post rdFobuf %d\n", zoslot);
			dprintf(stderr, "x[EOB]\n");
			err = sem_post(&ctx->rdFobuf);
			if ((rd == 0) && obuf->last) {
				/*
								close(pfd);
				 */
				dprintf(stderr, "\nlz4Thread: done %d\n", zoslot);
				pthread_exit(0);
			}
			assert(err == 0);
			zoslot++;
			if (ctx->o_entries == zoslot)
				zoslot = 0;
			dprintf(stderr, "lz4 wait wrTobuf %d\n", zoslot);
			err = sem_wait(&ctx->wrTobuf);
			obuf = ctx->o_buffer[zoslot];
			wr = 0;
		}
	}
}

static void *
lbuffer_output_pthread(void *args)
{
	int err;
	zfs_lbuffer_t *ctx = (zfs_lbuffer_t *) args;
	lbuffer_entry_t *obuf;
	size_t rest;
	uint32_t oslot = 0;

	dprintf(stderr, "OUTPUT: starting...\n");

	for (;;) {
		dprintf(stderr, "OUTPUT wait rdFobuf %d\n", oslot);
		err = sem_wait(&ctx->rdFobuf);
		dprintf(stderr, "OUTPUT got wait rdFobuf %d\n", oslot);
		obuf = ctx->o_buffer[oslot];
		rest = obuf->bsize;
		//		dprintf(stderr, "\nto_write: %d %d l:%d\n", oslot, obuf->bsize, obuf->last);
		if (ctx->terminate) {
			dprintf(stderr, "OUTPUT: terminate...\n");
			//(void) pthread_cancel(ctx->reader_tid);
			/* TODO handle close return code */
			//close(ctx->output_fd);
			pthread_exit(0);
		}
		if (0 == obuf->bsize && obuf->last) {
			exit_output_pthread(ctx);
		}
		do {
			int num;
			{
				num = write(ctx->output_fd, obuf->buf + obuf->bsize - rest, rest);
				dprintf(stderr, "OUTPUT write %d %d\n", oslot, num);
			}
			if ((-1 == num) && ((errno == ENOMEM) || (errno == ENOSPC))) {
				continue;
			} else if (-1 == num) {
				dprintf(stderr, "OUTPUT : error writing at %s\n", strerror(errno));
				ctx->terminate = 1;
				dprintf(stderr, "OUTPUT post wrTobuf %d\n", oslot);
				err = sem_post(&ctx->wrTobuf);
				assert(err == 0);
				pthread_exit((void *) - 1);
			}
			rest -= num;
			//dprintf(stderr, "rest %ld\n", rest);
		} while (rest > 0);
		//dprintf(stderr, "last:%d\n", lbuf->last);
		if (obuf->last) {
			exit_output_pthread(ctx);
		}
		dprintf(stderr, "OUTPUT wrTobuf %d\n", oslot);
		err = sem_post(&ctx->wrTobuf);
		assert(err == 0);
		oslot++;
		if (ctx->o_entries == oslot)
			oslot = 0;
	}
}

zfs_lbuffer_t *
libzfs_lbuffer_alloc(uint64_t buffer_size, buffermode_t mode)
{
	uint32_t entry;

	/* allocate buffering context */
	zfs_lbuffer_t *ctx = malloc(sizeof (zfs_lbuffer_t));
	if (ctx == NULL) {
		return NULL;
	}

	memset((void *) ctx, 0, sizeof (zfs_lbuffer_t));

	/* allocate buffer memory */
	if (mode == INPUT_BUFFER) {
		ctx->o_entries = 1 + buffer_size / BUFFER_ENTRY_SIZE;
		ctx->o_buffer = malloc(sizeof (uint8_t *) * ctx->o_entries);
		ctx->i_entries = 4;
		ctx->i_buffer = malloc(sizeof (uint8_t *) * ctx->i_entries);
	} else {
		ctx->i_entries = 1 + buffer_size / BUFFER_ENTRY_SIZE;
		ctx->i_buffer = malloc(sizeof (uint8_t *) * ctx->i_entries);
		ctx->o_entries = 4;
		ctx->o_buffer = malloc(sizeof (uint8_t *) * ctx->o_entries);
	}
	dprintf(stderr, "i_entries=%d, o_entries=%d\n", ctx->i_entries, ctx->o_entries);
	if (ctx->i_buffer == NULL || ctx->o_buffer == NULL) {
		goto lbuffer_nomem;
	}
	for (entry = 0; entry < ctx->i_entries; entry++) {
		ctx->i_buffer[entry] = (lbuffer_entry_t *) malloc(sizeof (lbuffer_entry_t));
		if (ctx->i_buffer[entry] == NULL) {
			goto lbuffer_nomem;
		}
	}
	for (entry = 0; entry < ctx->o_entries; entry++) {
		ctx->o_buffer[entry] = (lbuffer_entry_t *) malloc(sizeof (lbuffer_entry_t));
		if (ctx->o_buffer[entry] == NULL) {
			goto lbuffer_nomem;
		}
	}
	return ctx;

	/* free allocated memory */
lbuffer_nomem:
	if (ctx->i_buffer != NULL) {
		for (entry = 0; entry < ctx->i_entries; entry++) {
			if (ctx->i_buffer[entry] != NULL) {
				free(ctx->i_buffer[entry]);
				ctx->i_buffer[entry] = NULL;
			}
		}
		free(ctx->i_buffer);
	}
	if (ctx->o_buffer != NULL) {
		for (entry = 0; entry < ctx->o_entries; entry++) {
			if (ctx->o_buffer[entry] != NULL) {
				free(ctx->o_buffer[entry]);
				ctx->o_buffer[entry] = NULL;
			}
		}
		free(ctx->o_buffer);
	}
	free(ctx);
	return NULL;
}

static void
cancel_threads(zfs_lbuffer_t *ctx, int abort)
{
	ctx->finished = 1;
	pthread_join(ctx->reader_tid, 0);
	dprintf(stderr, "free joined reader\n");
	if (abort) {
		(void) pthread_cancel(ctx->writer_tid);
	}
	pthread_join(ctx->lza_tid, 0);
	dprintf(stderr, "free joined lz4\n");
	pthread_join(ctx->writer_tid, 0);
	dprintf(stderr, "free joined writer\n");
}

void
libzfs_lbuffer_free(zfs_lbuffer_t *ctx, int abort)
{
	uint32_t entry;

	if (ctx->i_buffer == NULL) {
		return;
	}

	cancel_threads(ctx, abort);

	for (entry = 0; entry < ctx->i_entries; entry++) {
		if (ctx->i_buffer[entry] != NULL) {
			free(ctx->i_buffer[entry]);
			ctx->i_buffer[entry] = NULL;
		}
	}
	free(ctx->i_buffer);
	for (entry = 0; entry < ctx->o_entries; entry++) {
		if (ctx->o_buffer[entry] != NULL) {
			free(ctx->o_buffer[entry]);
			ctx->o_buffer[entry] = NULL;
		}
	}
	free(ctx->o_buffer);
	if (ctx->workmem != NULL) {
		free(ctx->workmem);
		ctx->workmem = NULL;
	}
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

	ctx->buffer_mode = INPUT_BUFFER;
	ctx->input_fd = pipefd[0];
	ctx->output_fd = in_fd;

	err = sem_init(&ctx->wrTibuf, 0, ctx->i_entries);
	err = sem_init(&ctx->rdFibuf, 0, 0);
	err = sem_init(&ctx->wrTobuf, 0, ctx->o_entries);
	err = sem_init(&ctx->rdFobuf, 0, 0);

	err = pthread_create(&ctx->writer_tid, 0, &lbuffer_output_pthread, ctx);
	err = pthread_create(&ctx->reader_tid, 0, &lbuffer_input_pthread, ctx);
	if (ctx->compress) {
		err = pthread_create(&ctx->lza_tid, 0, &lbuffer_compress_pthread, ctx);
	} else {
		err = pthread_create(&ctx->lza_tid, 0, &lbuffer_forward_pthread, ctx);
	}
	return pipefd[1];
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

	ctx->buffer_mode = OUTPUT_BUFFER;
	ctx->input_fd = out_fd;
	ctx->output_fd = pipefd[1];

	err = sem_init(&ctx->rdFibuf, 0, 0);
	err = sem_init(&ctx->wrTibuf, 0, ctx->i_entries);
	err = sem_init(&ctx->wrTobuf, 0, ctx->o_entries);
	err = sem_init(&ctx->rdFobuf, 0, 0);

	err = pthread_create(&ctx->writer_tid, 0, &lbuffer_output_pthread, ctx);
	err = pthread_create(&ctx->reader_tid, 0, &lbuffer_input_pthread, ctx);
	err = pthread_create(&ctx->lza_tid, 0, &lbuffer_uncompress_pthread, ctx);
	return pipefd[0];
}
