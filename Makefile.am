ACLOCAL_AMFLAGS = -I m4
AUTOMAKE_OPTIONS = foreign
SUBDIRS = src man

undef.h: config.h.in
	grep -v config.h.in config.h.in > $@

debs: undef.h
	lversion=$$(echo @VERSION@ | tr '_' '-'); \
	cversion=$$(cat debian/changelog | grep -m 1 -Po '^ltsm.*\(\K[^)]+'); \
	if [ "$$lversion" != "$$cversion" ]; then \
		echo -e "1i\nltsm ($$lversion) unstable; urgency=low\n\n  * Automated changelog entry update\n\n -- Thomas Stibor <t.stibor@gsi.de>  $$(date -R)\n\n.\nwq" | ed debian/changelog; \
	fi; \
	rm -rf debs
	dpkg-buildpackage -us -uc -I.git -I\*.out[0-9]\* -I\*.swp || { \
		rc=$${PIPESTATUS[0]}; \
		[ $${rc} -gt 1 ] && exit $${rc}; \
		exit 0; \
	}

clean-local: clean-local-check
.PHONY: clean-local-check
clean-local-check:
	-rm -rf debian/ltsm

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
MAINTAINERCLEANFILES += configure
MAINTAINERCLEANFILES += compile
MAINTAINERCLEANFILES += depcomp
MAINTAINERCLEANFILES += install-sh
MAINTAINERCLEANFILES += missing
MAINTAINERCLEANFILES += ltmain.sh
MAINTAINERCLEANFILES += Makefile.in
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
MAINTAINERCLEANFILES += dsierror.log
MAINTAINERCLEANFILES += dsmerror.log
MAINTAINERCLEANFILES += debian/files
MAINTAINERCLEANFILES += debian/ltsm.debhelper.log
MAINTAINERCLEANFILES += debian/ltsm.postinst.debhelper
MAINTAINERCLEANFILES += debian/ltsm.postrm.debhelper
MAINTAINERCLEANFILES += debian/ltsm.prerm.debhelper
MAINTAINERCLEANFILES += debian/ltsm.substvars
