MAINTAINERCLEANFILES = Makefile.in config.h.in
AM_CFLAGS = $(fuse_CFLAGS) $(gstreamer_CFLAGS) $(sqlite3_CFLAGS)

bin_PROGRAMS	= dmxfs
dmxfs_SOURCES = dmxfs.c dmxfs_cap.c dmxfs_file.c
dmxfs_LDADD = $(fuse_LIBS) $(gstreamer_LIBS) $(sqlite3_LIBS)
