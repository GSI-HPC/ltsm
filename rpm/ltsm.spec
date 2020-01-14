%define name ltsm
%define version %{_version}
%define release %{_release}
%define _etcdir /etc

Summary: Lustre TSM copytool, TSM console client and Lustre TSM File System Daemon.
Name: %{name}
Version: %{version}
Release: %{release}
Source0: rpm/SOURCES/%{name}-%{version}-%{release}.tar.gz
License: GPLv2
BuildRoot: %{_tmppath}/%{name}-buildroot
BuildArch: x86_64
Vendor: GSI
Packager: Thomas Stibor
Url: http://github.com/tstibor/ltsm
Requires: TIVsm-API64 >= 7, lustre-client >= 2.10
BuildRequires: systemd, lustre-client >= 2.10

%description
Lustre TSM copytool for seamlessly archiving and retrieving data in Lustre
mount points. In addition a TSM console client is provided for archiving,
retrieving, deleting and querying data. This is especially useful when
a Lustre storage deployment is decommissioned and the archived data still
needs to be retrieved afterwards. Moreover, the package consists of a file system
daemon (called ltsmfsd) that implements a protocol for receiving data via socket,
copy data to Lustre and finally archive data on TSM server.

%prep
%autosetup -n %{name}-%{version}-%{release}

%build
./configure %{?configure_flags} --mandir=%{_mandir} --libdir=%{_libdir} --bindir=%{_bindir} --sbindir=%{_sbindir}
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}
mkdir -p %{buildroot}/%{_unitdir}
mkdir -p %{buildroot}/%{_etcdir}/default
make install DESTDIR=%{buildroot}
install -m 644 debian/%{name}.lhsmtool_tsm.service %{buildroot}/%{_unitdir}/%{name}.lhsmtool_tsm.service
install -m 600 debian/lhsmtool_tsm.default %{buildroot}/%{_etcdir}/default/lhsmtool_tsm
install -m 644 debian/%{name}.ltsmfsd.service %{buildroot}/%{_unitdir}/%{name}.ltsmfsd.service

%files
%defattr(-,root,root)
%{_mandir}/man1/lhsmtool_tsm.1.*
%{_mandir}/man1/ltsmc.1.*
%{_mandir}/man1/ltsmfsd.1.*
%{_bindir}/ltsmc
%{_sbindir}/lhsmtool_tsm
%{_sbindir}/ltsmfsd
%{_unitdir}/%{name}.lhsmtool_tsm.service
%{_etcdir}/default/lhsmtool_tsm
%{_unitdir}/%{name}.ltsmfsd.service

%post
%systemd_post %{name}.lhsmtool_tsm.service
%systemd_post %{name}.ltsmfsd.service

%preun
%systemd_preun %{name}.lhsmtool_tsm.service
%systemd_preun %{name}.ltsmfsd.service

%postun
%systemd_postun %{name}.lhsmtool_tsm.service
%systemd_postun %{name}.ltsmfsd.service

%clean
rm -rf %{buildroot}

%changelog

* Thu Aug 8 2018 Thomas Stibor <t.stibor@gsi.de> 0.7.3-1
- Lower and upper date/time bound for querying.
- Display in query results the CRC32 value.

* Thu Apr 5 2018 Thomas Stibor <t.stibor@gsi.de> 0.7.2-1
- Introduce and conf file option setting and man page update and corrections.

* Mon Dec 11 2017 Thomas Stibor <t.stibor@gsi.de> 0.7.1-1
- Update systemd requirements and fix archive_id handling
