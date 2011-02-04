# get rid of that stupid cache mechanism
rm -f config.cache

AC_INIT([dmxfs], [0.0.1], [])
AC_PREREQ([2.60])
AC_CONFIG_SRCDIR([configure.ac])
AC_CONFIG_MACRO_DIR([m4])
AC_CANONICAL_BUILD
AC_CANONICAL_HOST
AC_ISC_POSIX

AM_INIT_AUTOMAKE(1.6 dist-bzip2)
AM_CONFIG_HEADER(src/config.h)

AC_LIBTOOL_WIN32_DLL
define([AC_LIBTOOL_LANG_CXX_CONFIG], [:])dnl
define([AC_LIBTOOL_LANG_F77_CONFIG], [:])dnl
AC_PROG_LIBTOOL

VMAJ=`echo $PACKAGE_VERSION | awk -F. '{printf("%s", $1);}'`
VMIN=`echo $PACKAGE_VERSION | awk -F. '{printf("%s", $2);}'`
VMIC=`echo $PACKAGE_VERSION | awk -F. '{printf("%s", $3);}'`
SNAP=`echo $PACKAGE_VERSION | awk -F. '{printf("%s", $4);}'`
version_info=`expr $VMAJ + $VMIN`":$VMIC:$VMIN"
AC_SUBST(version_info)

AC_PROG_CC
AM_PROG_CC_STDC
AC_HEADER_STDC
AC_C_CONST
AC_C_BIGENDIAN

AC_CHECK_HEADER([sys/inotify.h], [have_inotify=yes],[have_inotify=no])
if test "x$have_inotify" = "xyes"; then
	AC_DEFINE(HAVE_INOTIFY, [1], [Build support for inotify])
fi
AM_CONDITIONAL(HAVE_INOTIFY, test "x$have_inotify" = "xyes")

# Checks for packages which use pkg-config.
PKG_CHECK_MODULES([fuse], [fuse >= 2.6.0])
PKG_CHECK_MODULES([sqlite3], [sqlite3])
PKG_CHECK_MODULES([gstreamer], [gstreamer-0.10])

AC_OUTPUT([
Makefile
src/Makefile
],[
])

#####################################################################
## Info

echo
echo
echo
echo "------------------------------------------------------------------------"
echo "$PACKAGE $VERSION"
echo "------------------------------------------------------------------------"
echo
echo
echo "Configuration Options Summary:"
echo "Installation Path...........................: ${prefix}"
echo "Features....................................:"
echo "  Inotify                                     ${have_inotify}"
echo
echo "Now type 'make' ('gmake' on some systems) to compile $PACKAGE,"
echo "and then afterwards as root (or the user who will install this), type"
echo "'make install'. Change users with 'su' or 'sudo' appropriately."
echo
