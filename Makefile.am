ACLOCAL_AMFLAGS = -I m4
AUTOMAKE_OPTIONS = foreign
SUBDIRS = src man

RPM_DIR=`pwd`/rpm

undef.h: config.h.in
	grep -v config.h.in config.h.in > $@

debs: undef.h Makefile
	gversion=$$(echo @VERSION@); \
	cversion=$$(cat debian/changelog | grep -m 1 -Po '^fsq.*\(\K[^)]+'); \
	if [ "$$gversion" != "$$cversion" ]; then \
		echo -e "1i\nfsq ($$gversion) unstable; urgency=low\n\n  * Automated changelog entry update\n\n -- Thomas Stibor <t.stibor@gsi.de>  $$(date -R)\n\n.\nwq" | ed debian/changelog; \
	fi; \
	rm -rf debs
	dpkg-buildpackage -us -uc -I.git -I\*.out[0-9]\* -I\*.swp || { \
		rc=$${PIPESTATUS[0]}; \
		[ $${rc} -gt 1 ] && exit $${rc}; \
		exit 0; \
	}

rpms: Makefile
	$(MAKE) $(AM_MAKEFLAGS)
	$(MKDIR_P) $(RPM_DIR)/{TMP,BUILD,RPMS,SRPMS,SPECS,SOURCES,BUILDROOT}; \
	_version=$$(echo @_VERSION@); \
	_release=$$(echo @_RELEASE@); \
	tar --transform "s/./fsq-$$_version-$$_release/" -c --exclude rpm -f - . | gzip > $(RPM_DIR)/SOURCES/fsq-$$_version-$$_release.tar.gz
	rpmbuild \
		--define "_topdir $(RPM_DIR)" \
		--define "_sourcedir $(RPM_DIR)/SOURCES" \
		--define "_tmppath $(RPM_DIR)/TMP" \
		--define "_version @_VERSION@" \
		--define "_release @_RELEASE@" \
		-ba rpm/fsq.spec

clean-local: clean-local-check
.PHONY: clean-local-check
clean-local-check:
	-rm -rf debian/{fsq,.debhelper} rpm/{TMP,BUILD,RPMS,SRPMS,SPECS,SOURCES,BUILDROOT} .cache

# Files to be deleted by 'make maintainer-clean'
MAINTAINERCLEANFILES  = aclocal.m4
MAINTAINERCLEANFILES += aminclude.am
MAINTAINERCLEANFILES += autom4te.cache/*
MAINTAINERCLEANFILES += $(AUX_DIST)
MAINTAINERCLEANFILES += config.log
MAINTAINERCLEANFILES += config.status
MAINTAINERCLEANFILES += config.sub
MAINTAINERCLEANFILES += config.guess
MAINTAINERCLEANFILES += config.h.in
MAINTAINERCLEANFILES += config.h.in~
MAINTAINERCLEANFILES += configure
MAINTAINERCLEANFILES += compile
MAINTAINERCLEANFILES += depcomp
MAINTAINERCLEANFILES += install-sh
MAINTAINERCLEANFILES += missing
MAINTAINERCLEANFILES += ltmain.sh
MAINTAINERCLEANFILES += Makefile.in
MAINTAINERCLEANFILES += man/Makefile.in
MAINTAINERCLEANFILES += src/Makefile.in
MAINTAINERCLEANFILES += src/lib/Makefile.in
MAINTAINERCLEANFILES += src/test/Makefile.in
MAINTAINERCLEANFILES += m4/libtool.m4
MAINTAINERCLEANFILES += m4/ltoptions.m4
MAINTAINERCLEANFILES += m4/ltsugar.m4
MAINTAINERCLEANFILES += m4/ltversion.m4
MAINTAINERCLEANFILES += m4/lt~obsolete.m4
MAINTAINERCLEANFILES += undef.h
MAINTAINERCLEANFILES += GPATH
MAINTAINERCLEANFILES += GRTAGS
MAINTAINERCLEANFILES += GSYMS
MAINTAINERCLEANFILES += GTAGS
MAINTAINERCLEANFILES += compile_commands.json
MAINTAINERCLEANFILES += dsierror.log
MAINTAINERCLEANFILES += dsmerror.log
MAINTAINERCLEANFILES += debian/files
MAINTAINERCLEANFILES += debian/fsq.debhelper.log
MAINTAINERCLEANFILES += debian/fsq.postinst.debhelper
MAINTAINERCLEANFILES += debian/fsq.postrm.debhelper
MAINTAINERCLEANFILES += debian/fsq.prerm.debhelper
MAINTAINERCLEANFILES += debian/fsq.substvars
MAINTAINERCLEANFILES += debian/autoreconf.after
MAINTAINERCLEANFILES += debian/autoreconf.before
MAINTAINERCLEANFILES += debian/debhelper-build-stamp
