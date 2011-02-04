#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <sys/statvfs.h>
#include <sqlite3.h>
#include <pthread.h>
#include <gst/gst.h>

#if HAVE_INOTIFY
#include <sys/inotify.h>
/* size of the event structure, not counting name */
#define EVENT_SIZE  (sizeof (struct inotify_event))
/* reasonable guess as to size of 1024 events */
#define BUF_LEN        1024 * (EVENT_SIZE + 16)
#endif

#include "dmxfs.h"

/*
 * The directory layout should be something like:
 * mountpoinr/CAPS
 * mountpoint/CAP/files
 * mountpount/CAP/(CAPS - CAP)/
 * mountpount/CAP/(CAPS - CAP)/files
 *
 * Possible tables:
 * Caps
 * +----+--------+
 * | id | string |
 * +----+--------+
 *
 * Files
 * +----+------+-------+
 * | id | path | mtime |
 * +----+------+-------+
 *
 * FileCaps
 * +----+---------+--------+
 * | id | file_id | cap_id |
 * +----+---------+--------+
 *
 *
 */

/*============================================================================*
 *                                  Local                                     *
 *============================================================================*/
typedef struct _dmxfs
{
	char *basepath;
	int verbose;
	sqlite3 *db;
	pthread_t scanner;
	struct {
		GstElement *pipeline;
		GstElement *source;
		GstElement *typefind;
		GstElement *fakesink;
	} typefind;
	struct {
		GstPipeline *pipeline;
		GstElement *src;
		GstElement *decodebin2;
		GstElement *fakesink;
	} uridecode;
#if HAVE_INOTIFY
	pthread_t monitor;
	int inotify_fd;
	int inotify_wd;
#endif
} dmxfs;

/******************************************************************************
 *                                 Database                                   *
 ******************************************************************************/
static int db_create_files(dmxfs *mfs)
{
	sqlite3_stmt *stmt;
	const char *tail;
	int error;

	error = sqlite3_prepare(mfs->db,
			"CREATE TABLE IF NOT EXISTS "
			"files(id INTEGER PRIMARY KEY AUTOINCREMENT, file TEXT UNIQUE, "
			"mtime INTEGER);",
			-1, &stmt, &tail);
	if (error != SQLITE_OK)
	{
		printf("Error creating the files database: %s\n", sqlite3_errmsg(mfs->db));
		return 0;
	}
	sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	return 1;
}

static int db_create_filecaps(dmxfs *mfs)
{
	sqlite3_stmt *stmt;
	const char *tail;
	int error;

	error = sqlite3_prepare(mfs->db,
			"CREATE TABLE IF NOT EXISTS "
			"filecaps(id INTEGER PRIMARY KEY AUTOINCREMENT, file INTEGER, cap INTEGER, "
			"FOREIGN KEY (file) REFERENCES files (id),"
			"FOREIGN KEY (cap) REFERENCES caps (id));",
			-1, &stmt, &tail);
	if (error != SQLITE_OK)
	{
		printf("Error creating the filecaps database: %s\n", sqlite3_errmsg(mfs->db));
		return 0;
	}
	sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	return 1;

}

static int db_setup(dmxfs *mfs)
{
	sqlite3_stmt *stmt;
	const char *tail;
	int error;
	sqlite3 *db;

	/* we should generate the database here
	 * in case it already exists, just
	 * compare mtimes of files
	 */
	if (sqlite3_open("/tmp/dmxfs.db", &mfs->db) != SQLITE_OK)
	{
		printf("could not open the db\n");
		return 0;
	}
	db = mfs->db;
	if (!cap_init(mfs->db))
	{
		printf("could not create the caps table\n");
		return 0;
	}
	if (!db_create_files(mfs))
	{
		printf("could not create the files table\n");
		return 0;
	}
	if (!db_create_filecaps(mfs))
	{
		printf("could not create the filecaps table\n");
		return 0;
	}

	return 1;
}

/* TODO move the db_*_[file, filecaps] into their own files */
static int db_insert_file(sqlite3 *db, const char *file, time_t mtime)
{
	char *str;
	sqlite3_stmt *stmt;
	const char *tail;
	int error;
	int id = -1;

	str = sqlite3_mprintf("INSERT OR IGNORE INTO files (file, mtime) VALUES ('%q',%d);",
			file, mtime);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	sqlite3_free(str);
	if (error != SQLITE_OK)
	{
		printf("1 error file %s\n", file);
		return id;
	}
	sqlite3_step(stmt);
	sqlite3_finalize(stmt);
	/* get the id */
	str = sqlite3_mprintf("SELECT id FROM files WHERE file = '%q';",
			file);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	sqlite3_free(str);
	if (error != SQLITE_OK)
	{
		printf("error artist %s\n", file);
		return id;
	}
	if (sqlite3_step(stmt) != SQLITE_ROW)
	{
		printf("error querying id\n");
		return id;
	}
	id = sqlite3_column_int(stmt, 0);
	sqlite3_finalize(stmt);

	return id;
}

static void db_insert_filecap(sqlite3 *db, int file_id, int cap_id)
{
	sqlite3_stmt *stmt;
	char *str;
	const char *tail;
	int error;
	int count;

	/* FIXME first check that there's no other row with the same cap and file
	 * we can handle this with the same constraints of the table
	 * should we create an "active record" for this too?
	 */
	str = sqlite3_mprintf("SELECT COUNT(*) FROM filecaps WHERE file = %d AND cap = %d;",
			file_id, cap_id);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	sqlite3_free(str);
	if (error != SQLITE_OK)
	{
		printf("1 error caps %d %d\n", file_id, cap_id);
	}
	sqlite3_step(stmt);
	count = sqlite3_column_int(stmt, 0);
	if (count)
	{
		printf("already exists\n");
		return;
	}
	sqlite3_finalize(stmt);

	str = sqlite3_mprintf("INSERT OR IGNORE INTO filecaps (file, cap) VALUES (%d, %d);",
			file_id, cap_id);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	sqlite3_free(str);
	if (error != SQLITE_OK)
	{
		printf("1 error caps %d %d\n", file_id, cap_id);
		return;
	}
	sqlite3_step(stmt);
	sqlite3_finalize(stmt);
}

static int db_file_changed(sqlite3 *db, const char *file, time_t mtime)
{
	char *str;
	sqlite3_stmt *stmt;
	const char *tail;
	int error;

	str = sqlite3_mprintf("SELECT mtime FROM files WHERE file = '%q'", file);
	/* check if the file exists if so check the mtime and compare */
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	sqlite3_free(str);
	if (error != SQLITE_OK)
	{
		printf("1 error file %s\n", file);
		return 1;
	}
	if (sqlite3_step(stmt) != SQLITE_ROW)
	{
		return 1;
	}
	else
	{
		time_t dbtime;

		dbtime = sqlite3_column_int(stmt, 0);
		sqlite3_finalize(stmt);
		if (dbtime < mtime)
		{
			/* FIXME file has chaned, delete every entry? */
			return 1;
		}
		else
			return 0;
	}
}

/******************************************************************************
 *                                Scanner                                     *
 ******************************************************************************/
static void setup_typefind_pipeline(dmxfs *mfs)
{
	GstElement *pipeline;
	GstElement *source;
	GstElement *typefind;
	GstElement *fakesink;
	
	pipeline = gst_pipeline_new ("pipeline");
	source = gst_element_factory_make ("filesrc", "source");
	typefind = gst_element_factory_make ("typefind", "typefind");
	fakesink = gst_element_factory_make ("fakesink", "fakesink");

	gst_bin_add_many (GST_BIN (pipeline), source, typefind, fakesink, NULL);
	gst_element_link_many (source, typefind, fakesink, NULL);

	mfs->typefind.pipeline = pipeline;
	mfs->typefind.source = source;
	mfs->typefind.typefind = typefind;
	mfs->typefind.fakesink = fakesink;
}

static void cleanup_typefind_pipeline(dmxfs *mfs)
{
	gst_object_unref (mfs->typefind.pipeline);
	mfs->typefind.pipeline = NULL; 
	mfs->typefind.source = NULL;
	mfs->typefind.typefind = NULL;
	mfs->typefind.fakesink = NULL;
}

static void on_have_type (GstElement * typefind, guint probability,
    const GstCaps * caps, GstCaps ** p_caps)
{
	if (p_caps) {
		*p_caps = gst_caps_copy (caps);
	}
}

static int is_media(dmxfs *mfs, char *file, time_t mtime)
{
	GstCaps *caps = NULL;
	GstState state;
	GstStateChangeReturn sret;
	int ret = 0;
	gulong handler;

	handler = g_signal_connect (G_OBJECT (mfs->typefind.typefind), "have-type",
			G_CALLBACK (on_have_type), &caps);

	g_object_set (mfs->typefind.source, "location", file, NULL);

	gst_element_set_state (GST_ELEMENT (mfs->typefind.pipeline), GST_STATE_PAUSED);
	sret = gst_element_get_state (GST_ELEMENT (mfs->typefind.pipeline), &state, NULL, -1);

	switch (sret)
	{
		case GST_STATE_CHANGE_FAILURE:
		break;

		case GST_STATE_CHANGE_SUCCESS:
		if (caps)
		{
			guint num;
			int i;

			num = gst_caps_get_size(caps);
			for (i = 0; i < num; i++)
			{
				GstStructure *st;
				const gchar *name;

				st = gst_caps_get_structure(caps, i);
				/* insert the name to the database */
				name = gst_structure_get_name(st);
				/* check that we have a valid caps first */
				if (!strncmp(name, "video", 5) || !strncmp(name, "audio", 5)
						|| !strncmp(name, "application/ogg", 15)
						|| !strncmp(name, "application/x-id3", 17))
				{
					printf("Caps found %s\n", name);
					/* now insert the file */
					ret = db_insert_file(mfs->db, file, mtime);
					/* get the id, and return it */
					gst_caps_unref (caps);
					break;
				}
			}
		}
		break;

		default:
		break;
	}
	g_signal_handler_disconnect(G_OBJECT (mfs->typefind.typefind), handler);
	gst_element_set_state (mfs->typefind.pipeline, GST_STATE_NULL);

	return ret;
}

static void setup_uridecode_pipeline(dmxfs *mfs)
{
	GstPipeline *pipeline;
	GstElement *src;
	GstElement *decodebin2;
	GstElement *fakesink;

	pipeline = GST_PIPELINE(gst_pipeline_new(NULL));
	src = gst_element_factory_make("filesrc", NULL);
	decodebin2 = gst_element_factory_make("decodebin2", NULL);
	fakesink = gst_element_factory_make("fakesink", NULL);

	gst_bin_add_many(GST_BIN(pipeline), src, decodebin2, NULL);
	gst_element_link(src, decodebin2);
	//gst_bin_add_many(GST_BIN(pipeline), src, decodebin2, fakesink, NULL);
	//gst_element_link_many(src, decodebin2, fakesink, NULL);

	mfs->uridecode.pipeline = pipeline;
	mfs->uridecode.src = src;
	mfs->uridecode.decodebin2 = decodebin2;
	mfs->uridecode.fakesink = fakesink;
}

static void cleanup_uridecode_pipeline(dmxfs *mfs)
{
	gst_object_unref(mfs->uridecode.pipeline);
}

static gboolean on_autoplug_continue(GstBin *bin, GstPad *pad, GstCaps *caps,
			gpointer user_data)
{
	GList **list = (GList **)user_data;
	GstStructure *st;
	const gchar *name;
	int num;
	int i;

	num = gst_caps_get_size(caps);
	for (i = 0; i < num; i++)
	{
		st = gst_caps_get_structure(caps, 0);
		name = gst_structure_get_name(st);
		if (strstr(name, "raw"))
			return TRUE;
	}
	
	*list = g_list_append(*list, gst_caps_ref(caps));
	return TRUE;
}

static void get_caps(dmxfs *mfs, char *file, int file_id)
{
	GstState state;
	GstClockTime clock;
	GstStateChangeReturn sret;
	GList *caps = NULL;
	GList *tmp;
	int cap_id;
	guint num;
	int i;
	gulong handler;

	g_object_set(G_OBJECT(mfs->uridecode.src), "location", file, NULL);
	handler = g_signal_connect(G_OBJECT(mfs->uridecode.decodebin2), "autoplug-continue",
			G_CALLBACK(on_autoplug_continue), &caps);
	gst_element_set_state(GST_ELEMENT(mfs->uridecode.pipeline), GST_STATE_PLAYING);

	/* wait until state change either completes or fails */
	printf("before changing state\n");
	//sret = gst_element_get_state(GST_ELEMENT(mfs->uridecode.pipeline), &state, NULL, -1);
	clock = (GstClockTime)3 * GST_SECOND;
	sret = gst_element_get_state(GST_ELEMENT(mfs->uridecode.pipeline), &state, NULL, clock);
	printf("after changing state %d\n", sret);
	switch (sret) {
		case GST_STATE_CHANGE_FAILURE:
		break;

		case GST_STATE_CHANGE_SUCCESS:
		tmp = caps;
		while (tmp)
		{
			GstCaps *cap = tmp->data;
			num = gst_caps_get_size(cap);
			for (i = 0; i < num; i++)
			{
				GstStructure *st;
				Cap *mcap;
				const gchar *name;
				char *tmp1;
				char *tmp2;

				st = gst_caps_get_structure(cap, i);
				/* insert the name to the database */
				name = gst_structure_get_name(st);
				/* replace the slashes with underscores */
				tmp1 = strdup(name);
				for (tmp2 = tmp1; tmp2 && *tmp2; tmp2++)
				{
					if (*tmp2 == '/') *tmp2 = '_';
				}
				mcap = cap_new_from_name(mfs->db, tmp1);
				printf("2 Adding cap %s\n", tmp1);
				free(tmp1);
				/* add this cap and file to the filecaps table */
				db_insert_filecap(mfs->db, file_id, mcap->id);
				cap_free(mcap);
			}
			gst_caps_unref(cap);
			tmp = tmp->next;
		}
		break;

		default:
		break;
	}
	g_signal_handler_disconnect(G_OBJECT (mfs->uridecode.decodebin2), handler);
	gst_element_set_state(GST_ELEMENT(mfs->uridecode.pipeline), GST_STATE_NULL);
	g_list_free(caps);
}

static void _scan(const char *path, dmxfs *mfs)
{
	DIR *dp;
	struct dirent *de;

	printf("scanning %s\n", path);
	dp = opendir(path);
	if (!dp)
	{
		printf("cannot scan dir\n");
		return;
	}

	while ((de = readdir(dp)) != NULL)
	{
		char realfile[PATH_MAX];
		struct stat st;

		if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, ".."))
			continue;

		/* also check subdirs */
		strncpy(realfile, path, PATH_MAX);
		strncat(realfile, "/", PATH_MAX - strlen(de->d_name));
		strncat(realfile, de->d_name, PATH_MAX - strlen(de->d_name));

		if (stat(realfile, &st) < 0)
		{
			printf("err on stat %d %s\n", errno, realfile);
			continue;
		}

		if (S_ISDIR(st.st_mode))
		{
			_scan(realfile, mfs);
		}
		else if (S_ISREG(st.st_mode))
		{
			int id;
			char *str;
			void *handle;

			printf("processing file %s\n", realfile);
			if (!db_file_changed(mfs->db, realfile, st.st_mtime))
			{
				printf("file didnt change, nothing to do\n");
				continue;
			}
			id = is_media(mfs, realfile, st.st_mtime);
			printf("media found? %d\n", id);
			if (id > 0) {
				get_caps(mfs, realfile, id);
			}
		}
	}
	closedir(dp);
}

static void * _scanner(void *data)
{
	dmxfs *mfs = data;

	_scan(mfs->basepath, mfs);
	return NULL;
}


static void dmxfs_scan(dmxfs *mfs)
{
	int ret;
	pthread_attr_t attr;

	ret = pthread_attr_init(&attr);
	if (ret) {
		perror("pthread_attr_init");
		return;
	}

	ret = pthread_create(&mfs->scanner, &attr, _scanner, mfs);
	if (ret) {
		perror("pthread_create");
		return;
	}
}

/******************************************************************************
 *                                Monitor                                     *
 ******************************************************************************/
#if HAVE_INOTIFY
static void * _monitor(void *data)
{
#if 0
	dmxfs *mfs = data;

	printf("starting the monitor\n");
	mfs->inotify_fd = inotify_init();
	if (mfs->inotify_fd < 0)
	{
		printf("error initializing inotify\n");
		return NULL;
	}
	mfs->inotify_wd = inotify_add_watch(mfs->inotify_fd, mfs->basepath, IN_MODIFY | IN_CREATE | IN_DELETE);
	//mfs->inotify_wd = inotify_add_watch(mfs->inotify_fd, mfs->basepath, IN_ALL_EVENTS);
	if (mfs->inotify_wd < 0)
	{
		printf("error adding the watch\n");
		return NULL;
	}
	while (1)
	{
		char buf[BUF_LEN];
		int len, i = 0;

		len = read(mfs->inotify_fd, buf, BUF_LEN);
		while (i < len)
		{
			struct inotify_event *event;

		        event = (struct inotify_event *) &buf[i];
			printf("wd=%d mask=%u cookie=%u len=%u\n",
        	        	event->wd, event->mask,
	                	event->cookie, event->len);

        		if (event->len)
		                printf ("name=%s\n", event->name);

		        i += EVENT_SIZE + event->len;
		}
	}
	//inotify_rm_watch(mfs->inotify_fd, mfs->inotify_wd);
	//close(mfs->inotify_fd);
#endif
}

static void dmxfs_monitor(dmxfs *mfs)
{
#if 0
	int ret;
	pthread_attr_t attr;

	ret = pthread_attr_init(&attr);
	if (ret) {
		perror("pthread_attr_init");
		return;
	}

	ret = pthread_create(&mfs->monitor, &attr, _monitor, mfs);
	if (ret) {
		perror("pthread_create");
		return;
	}
#endif
}
#endif

/******************************************************************************
 *                                   FUSE                                     *
 ******************************************************************************/
static int dmxfs_readlink(const char *path, char *buf, size_t size)
{
	File *file;
	dmxfs *mfs;
	struct fuse_context *ctx;
	char *tmp;

	/* get the last entry on the path */
	for (tmp = path + strlen(path); tmp--; tmp != path)
	{
		if (*tmp == '/')
		{
			tmp++;
			break;
		}
	}

	ctx = fuse_get_context();
	mfs = ctx->private_data;

	file = file_get_from_id(mfs->db, atoi(tmp));
	if (!file) return -ENOENT;

	strncpy(buf, file->name, size);
	buf[size - 1] = '\0';

	return 0;
}

static void path_to_caps(sqlite3 *db, const char *path, GList **caps)
{
	char *token;
	char *tmp;

	tmp = strdup(path);
	token = strtok(tmp, "/");
	while (token)
	{
		Cap *cap;

		cap = cap_get_from_name(db, token);
		if (!cap) break;
		*caps = g_list_append(*caps, cap);
		token = strtok(NULL, "/");
	}
	free(tmp);
}

static int path_remove_files(const char *path, char **new_path)
{
	char *last;

	printf("removing files\n");
	last = rindex(path, '/');
	if (!last)
	{
		*new_path = strdup(path);
		return 0;
	}

	if (!strncmp(last + 1, "files", 5))
	{
		*new_path = strndup(path, path - last);
		return 1;
	}
	*new_path = strdup(path);
	return 0;
}

static int dmxfs_readdir_old(const char *path, void *buf, fuse_fill_dir_t filler,
		off_t offset, struct fuse_file_info *fi)
{
	dmxfs *mfs;
	struct fuse_context *ctx;
	GList *caps_path = NULL;
	GList *caps = NULL;
	GList *files = NULL;
	GList *l;
	char *real_path;
	int is_files = 0;

	ctx = fuse_get_context();
	mfs = ctx->private_data;
	
	printf("reading dir %s with offset %d\n", path, offset);
	/* add simple '.' and '..' files */
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	/* check if the path ends with files, if so, go to files */
	is_files = path_remove_files(path, &real_path);
	path_to_caps(mfs->db, real_path, &caps_path);

	if (!is_files)
	{
		/* check if there are some subdirs */
		caps = cap_get_different_from_caps(mfs->db, caps_path);
		if (caps)
		{
			filler(buf, "files", NULL, 0);
		}
		for (l = caps; l; l = l->next)
		{
			Cap *cap;

			cap = l->data;
			if (filler(buf, cap->name, NULL, 0))
				break;
			cap_free(cap);
		}
	}
	else
	{
		/* get the list of files for the given caps */
		files = file_get_from_caps(mfs->db, caps_path, 0, -1);
		for (l = files; l; l = l->next)
		{
			File *file;
			char tmp[PATH_MAX];

			file = l->data;
			snprintf(tmp, PATH_MAX, "%08d", file->id);
			if (filler(buf, tmp, NULL, 0))
				break;
			file_free(file);
		}
	}
	free(real_path);
	/* destroy the lists */
	if (files)
		g_list_free(files);

	if (caps)
		g_list_free(caps);

	for (l = caps_path; l; l = l->next)
	{
		Cap *cap = l->data;
		cap_free(cap);
	}
	if (caps_path)
		g_list_free(caps_path);

	return 0;
}
#if 0
static int dmxfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		off_t offset, struct fuse_file_info *fi)
{
	dmxfs *mfs;
	struct fuse_context *ctx;
	GList *caps_path = NULL;
	GList *caps = NULL;
	GList *files = NULL;
	GList *l;
	int off = offset;
	int limit = 100;
	int caps_num;

	ctx = fuse_get_context();
	mfs = ctx->private_data;
	
	printf("reading dir %s with offset %d\n", path, offset);
	/* add simple '.' and '..' files */
	if (offset == 0)
	{
		filler(buf, ".", NULL, ++offset);
		return 0;
	}
	if (offset == 1)
	{
		filler(buf, "..", NULL, ++offset);
		return 0;
	}

	/* check if there are some subdirs */
	/* we can avoid here to get the number of rows from the table
	 * bceause the number caps is usually small
	 */
	path_to_caps(mfs->db, path, &caps_path);
	caps = cap_get_different_from_caps(mfs->db, caps_path);
	caps_num = 0;
	/* get the number of offset and check we are inside the caps or not */
	for (l = caps; l; l = l->next)
	{
		Cap *cap;

		cap = l->data;
		if (caps_num + 2 == offset)
		{
			printf("filling %d %d\n", caps_num + 2, offset);
			if (filler(buf, cap->name, NULL, ++offset))
				break;
		}
		caps_num++;
	}
	/* get the list of files for the given caps */
	off = offset - caps_num - 2;
	files = file_get_from_caps(mfs->db, caps_path, off, limit);
	for (l = files; l; l = l->next)
	{
		File *file;
		char tmp[PATH_MAX];

		file = l->data;
		snprintf(tmp, PATH_MAX, "%08d", file->id);
		if (filler(buf, tmp, NULL, ++offset))
			break;
		file_free(file);
	}
end:
	/* destroy the lists */
	if (files)
		g_list_free(files);

	for (l = caps; l; l = l->next)
	{
		Cap *cap = l->data;
		cap_free(cap);
	}
	if (caps)
		g_list_free(caps);

	for (l = caps_path; l; l = l->next)
	{
		Cap *cap = l->data;
		cap_free(cap);
	}
	if (caps_path)
		g_list_free(caps_path);

	return 0;
}
#endif

static int dmxfs_getattr(const char *path, struct stat *stbuf)
{
	struct fuse_context *ctx;
	dmxfs *mfs;
	int ret = 0;
	char *real_path;

	ctx = fuse_get_context();
	mfs = ctx->private_data;

	memset(stbuf, 0, sizeof(struct stat));
	if (!strcmp(path, "/"))
	{
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
	}
	else
	{
		char *tmp;

		/* check if the last entry is a files, if so, check for files */
		/* get the last entry on the path and check if it is a
		 * a valid file or a cap
		 */
		for (tmp = path + strlen(path); tmp--; tmp != path)
		{
			if (*tmp == '/')
			{
				tmp++;
				break;
			}
		}
		if (!strncmp(tmp, "files", 5))
		{
			stbuf->st_mode = S_IFDIR | 0755;
			stbuf->st_nlink = 2;
		}
		else
		{
			File *file;

			file = file_get_from_id(mfs->db, atoi(tmp));
			if (file)
			{
				stbuf->st_mode = S_IFLNK | 0755;
				stbuf->st_nlink = 1;
				file_free(file);
			}
			else
			{
				Cap *cap;
				cap = cap_get_from_name(mfs->db, tmp);
				if (!cap) ret = -ENOENT;
				else
				{
					stbuf->st_mode = S_IFDIR | 0755;
					stbuf->st_nlink = 2;
					cap_free(cap);
				}
			}
		}
	}
	return ret;
}

static int dmxfs_open(const char *path, struct fuse_file_info *fi)
{
	return 0;
}

static int dmxfs_read(const char *path, char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi)
{
	return 0;
}

static int dmxfs_statfs(const char *path, struct statvfs *stbuf)
{
	return 0;
}

/**
 * Here we handle all the logic of the mv operation
 */
int dmxfs_rename(const char *orig, const char *dest)
{
#if 0
	dmxfs_query src;
	dmxfs_query dst;
	char *tmp;
	int ret;

	tmp = strdup(orig);
	ret = _path_to_query(tmp, &src);
	free(tmp);

	tmp = strdup(dest);
	ret = _path_to_query(tmp, &dst);
	free(tmp);

	printf("rename %s %s!!!\n", orig, dest);
	_query_dump(&src);
	_query_dump(&dst);
#endif
	return -EACCES;
}

static void * dmxfs_init(struct fuse_conn_info *conn)
{
	struct fuse_context *ctx;
	dmxfs *mfs;

	/* setup the connection info */
	conn->async_read = 0;
	/* get the context */
	ctx = fuse_get_context();
	mfs = ctx->private_data;
	/* read/create the database */
	if (!db_setup(mfs)) return NULL;
	/* setup the gst pipelines */
	setup_typefind_pipeline(mfs);
	setup_uridecode_pipeline(mfs);
	/* update the database */
	dmxfs_scan(mfs);
	/* monitor file changes */
#if HAVE_INOTIFY
	dmxfs_monitor(mfs);
#endif
	return mfs;
}

static struct fuse_operations dmxfs_ops = {
	.getattr  = dmxfs_getattr,
	.readlink = dmxfs_readlink,
	.readdir  = dmxfs_readdir_old,
	.open     = dmxfs_open,
	.read     = dmxfs_read,
	.statfs   = dmxfs_statfs,
	.rename   = dmxfs_rename,
	.init     = dmxfs_init,
};
/******************************************************************************
 *                                 Helpers                                    *
 ******************************************************************************/
static void usage(void)
{
	printf("Usage:\n");
	printf("dmxfs FILE\n");
}

static void dmxfs_free(dmxfs *mfs)
{
	if (mfs->scanner)
	{
		pthread_cancel(mfs->scanner);
		pthread_join(mfs->scanner, NULL);
	}
#if HAVE_INOTIFY
	if (mfs->monitor)
	{
		pthread_cancel(mfs->monitor);
		pthread_join(mfs->monitor, NULL);
	}
#endif
	/* remove the pipelines */
	cleanup_typefind_pipeline(mfs);
	cleanup_uridecode_pipeline(mfs);

	free(mfs->basepath);
	free(mfs);
}

int main(int argc, char **argv)
{
	dmxfs *mfs;

	if (argc < 2)
	{
		usage();
		return 0;
	}

	mfs = calloc(1, sizeof(dmxfs));
	mfs->basepath = strdup(argv[1]);

	argv[1] = argv[0];

	gst_init(0, NULL);
	fuse_main(argc - 1, argv + 1, &dmxfs_ops, mfs);

	dmxfs_free(mfs);

	return 0;
}
