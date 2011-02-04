#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>
#include <sqlite3.h>
#include "dmxfs.h"
/*============================================================================*
 *                                  Local                                     *
 *============================================================================*/
static Cap * cap_new(unsigned int id, const char *name)
{
	Cap *cap;

	cap = malloc(sizeof(Cap));
	cap->id = id;
	cap->name = strdup(name);

	return cap;
}
/*============================================================================*
 *                                 Global                                     *
 *============================================================================*/
void cap_free(Cap *cap)
{
	free(cap->name);
	free(cap);
}

void cap_destroy(Cap *cap, sqlite3 *db)
{

}

Cap * cap_new_from_name(sqlite3 *db, const char *name)
{
	Cap *cap = NULL;
	sqlite3_stmt *stmt;
	char *str;
	const char *tail;
	int error;
	int id;

	str = sqlite3_mprintf("INSERT OR IGNORE INTO caps (name) VALUES ('%q');",
			name);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	sqlite3_free(str);
	if (error != SQLITE_OK)
	{
		printf("1 error caps %s\n", name);
		goto end;
	}
	sqlite3_step(stmt);
	sqlite3_finalize(stmt);
	/* get the id */	
	str = sqlite3_mprintf("SELECT id FROM caps WHERE name = '%q';",
			name);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	sqlite3_free(str);
	if (error != SQLITE_OK)
	{
		printf("1 error caps %s\n", name);
		goto end;
	}
	if (sqlite3_step(stmt) != SQLITE_ROW)
	{
		goto end;
	}
	id = sqlite3_column_int(stmt, 0);
	cap = cap_new(id, name);
end:
	sqlite3_finalize(stmt);

	return cap;

}

/**
 * Given a list of names return the caps found on the database
 * that are different from the provided names
 */
GList * cap_get_different_from_caps(sqlite3 *db, GList *caps)
{
	GList *ret = NULL;
	sqlite3_stmt *stmt;
	const char *tail;
	int error;
	char *query;
	char *str;

	if (caps)
	{
		Cap *cap = caps->data;
		GList *l = caps;
		char *substr;
		int count = 1;
		char *tmp;


		/* first generate the substring */
		/* FIXME we should avoid the duplicate from the caps list */
		/* select file as f from filecaps where cap in (SET) group by f having count(*) = NUM */
		substr = sqlite3_mprintf("SELECT file AS f FROM filecaps WHERE cap = %d", cap->id);
		for (l = l->next; l; l = l->next)
		{
			cap = l->data;
			tmp = sqlite3_mprintf("%s OR cap = %d", substr, cap->id);
			sqlite3_free(substr);
			substr = tmp;
			count++;
		}
		tmp = sqlite3_mprintf("%s GROUP BY f HAVING COUNT(*) = %d", substr, count);
		sqlite3_free(substr);
		substr = tmp;

		str = sqlite3_mprintf("SELECT DISTINCT caps.id, caps.name FROM caps, filecaps, (%s) WHERE filecaps.cap = caps.id AND filecaps.file = f", substr);
		sqlite3_free(substr);
		for (l = caps; l; l = l->next)
		{
			cap = l->data;
			tmp = sqlite3_mprintf("%s AND cap != %d", str, cap->id);
			sqlite3_free(str);
			str = tmp;
		}
	}
	else
	{
		str = sqlite3_mprintf("SELECT id, name FROM caps");
	}
	printf("cap query = %s\n", str);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	if (error != SQLITE_OK)
	{
		printf("Error on the query fetching caps %s\n", str);
		goto end;
	}
	while (sqlite3_step(stmt) == SQLITE_ROW)
	{
		Cap *cap;
		const unsigned char *name;
		unsigned int id;

		id = sqlite3_column_int(stmt, 0);
		name = sqlite3_column_text(stmt, 1);
		cap = cap_new(id, name);
		ret = g_list_append(ret, cap);
	}
end:
	if (str) sqlite3_free(str);
	sqlite3_finalize(stmt);

	return ret;
}

GList * cap_get_relative(GList *caps)
{
	/* get all caps that are relative to @caps, that is
	 * get the list of filecaps that have @caps
	 * then from the files ids, fetch all the filecaps with that
	 * ids and that are not part of @caps
	 * return resulting caps on a list
	 */
}

Cap * cap_get_from_name(sqlite3 *db, const char *name)
{
	Cap * cap = NULL;
	char *str;
	sqlite3_stmt *stmt;
	const char *tail;
	int error;
	int id = -1;

	str = sqlite3_mprintf("SELECT id FROM caps WHERE name = '%q'", name);
	error = sqlite3_prepare(db, str, -1, &stmt, &tail);
	sqlite3_free(str);
	if (error != SQLITE_OK)
	{
		printf("Error on the cap_get() query %s\n", name);
		goto end;
	}
	if (sqlite3_step(stmt) != SQLITE_ROW)
	{
		printf("Error querying cap id for %s\n", name);
		goto end;
	}
	id = sqlite3_column_int(stmt, 0);
	cap = cap_new(id, name);
end:
	sqlite3_finalize(stmt);

	return cap;
}

/**
 * Create the caps database in case it is not created yet
 */
int cap_init(sqlite3 *db)
{
	sqlite3_stmt *stmt;
	const char *tail;
	int error;

	error = sqlite3_prepare(db,
			"CREATE TABLE IF NOT EXISTS "
			"caps(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE);",
			-1, &stmt, &tail);
	if (error != SQLITE_OK)
	{
		printf("Error creating the caps database: %s\n", sqlite3_errmsg(db));
		return 0;
	}
	sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	return 1;
}
