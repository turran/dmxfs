#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <string.h>
#include <stdio.h>
#include <glib.h>
#include <sqlite3.h>
#include "dmxfs.h"
/*============================================================================*
 *                                  Local                                     *
 *============================================================================*/
static File * file_new(unsigned int id, const char *name)
{
	File *cap;

	cap = malloc(sizeof(File));
	cap->id = id;
	cap->name = strdup(name);

	return cap;
}
/*============================================================================*
 *                                 Global                                     *
 *============================================================================*/
void file_free(File *file)
{
	free(file->name);
	free(file);
}

GList * file_get_from_caps(sqlite3 *db, GList *caps, int offset, int limit)
{
	GList *files = NULL;
	char *str = NULL;
	sqlite3_stmt *stmt;
	const char *tail;
	int error;

	if (caps)
	{
		Cap *cap = caps->data;
		GList *l = caps;
		char *tmp;
		int count = 1;

		str = sqlite3_mprintf("SELECT files.id, files.file FROM files INNER JOIN filecaps on filecaps.file = files.id AND (filecaps.cap = %d", cap->id);
		for (l = l->next; l; l = l->next)
		{
			Cap *cap = l->data;

			tmp = sqlite3_mprintf("%s OR filecaps.cap = %d", str, cap->id);
			sqlite3_free(str);
			str = tmp;
			count++;
		}
		tmp = sqlite3_mprintf("%s) GROUP BY files.id HAVING COUNT(*) = %d LIMIT %d OFFSET %d", str, count, limit, offset);
		sqlite3_free(str);
		str = tmp;
	}
	else
	{
		str = sqlite3_mprintf("SELECT id, file FROM files LIMIT %d OFFSET %d", limit, offset);
	}
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	printf("query = %s\n", str);
	if (error != SQLITE_OK)
	{
		printf("Error on the query %s\n", str);
		goto end;
	}
	while (sqlite3_step(stmt) == SQLITE_ROW)
	{
		File *file;
		int id;
		const unsigned char *name;

		id = sqlite3_column_int(stmt, 0);
		name = sqlite3_column_text(stmt, 1);
		file = file_new(id, name);
		files = g_list_append(files, file);
	}
end:
	if (str) sqlite3_free(str);
	sqlite3_finalize(stmt);

	return files;
}
File * file_get_from_id(sqlite3 *db, unsigned int id)
{
	File *file = NULL;
	char *str = NULL;
	sqlite3_stmt *stmt;
	const char *tail;
	const char *name;
	int error;

	str = sqlite3_mprintf("SELECT file FROM files WHERE id = %d;",
			id);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	if (error != SQLITE_OK)
	{
		printf("Error on the file_get() query %s\n", name);
		goto end;
	}
	if (sqlite3_step(stmt) != SQLITE_ROW)
	{
		printf("Error querying file id for %s\n", name);
		goto end;
	}
	name = sqlite3_column_text(stmt, 0);
	file = file_new(id, name);
end:
	if (str) sqlite3_free(str);
	sqlite3_finalize(stmt);

	return file;
}

File * file_get_from_name(sqlite3 *db, const char *name)
{
	File *file = NULL;
	char *str = NULL;
	sqlite3_stmt *stmt;
	const char *tail;
	int error;
	int id;

	str = sqlite3_mprintf("SELECT id FROM files WHERE file = '%q';",
			name);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	if (error != SQLITE_OK)
	{
		printf("Error on the file_get() query %s\n", name);
		goto end;
	}
	if (sqlite3_step(stmt) != SQLITE_ROW)
	{
		printf("Error querying file id for %s\n", name);
		goto end;
	}
	id = sqlite3_column_int(stmt, 0);
	file = file_new(id, name);
end:
	if (str) sqlite3_free(str);
	sqlite3_finalize(stmt);

	return file;
}
