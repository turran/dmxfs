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
static FileCap * filecap_new(unsigned int id, Cap *cap, File *file)
{
	FileCap *fc;

	fc = malloc(sizeof(FileCap));
	fc->id = id;

	return fc;
}
/*============================================================================*
 *                                 Global                                     *
 *============================================================================*/
void filecap_free(FileCap *fc)
{
	free(fc);
}

