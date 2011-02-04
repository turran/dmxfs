#ifndef MEDIADBGFS_H_
#define MEDIADBGFS_H_

typedef struct _Cap
{
	unsigned int id;
	char *name;
} Cap;

void cap_free(Cap *cap);
void cap_destroy(Cap *cap, sqlite3 *db);
Cap * cap_new_from_name(sqlite3 *db, const char *name);
Cap * cap_get_from_name(sqlite3 *db, const char *name);
GList * cap_get_relative(GList *caps);
int cap_init(sqlite3 *db);
GList * cap_get_different_from_caps(sqlite3 *db, GList *caps);

typedef struct _File
{
	unsigned int id;
	char *name;
	time_t modtime;
} File;

File * file_get_from_id(sqlite3 *db, unsigned int id);
File * file_get_from_name(sqlite3 *db, const char *name);
GList * file_get_from_caps(sqlite3 *db, GList *caps, int limit, int offset);
void file_free(File *file);

#endif
