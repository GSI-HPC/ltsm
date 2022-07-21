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
needs to be retrieved afterwards.

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

%files
%defattr(-,root,root)
%doc script/ltsmsync.sh
%{_mandir}/man1/lhsmtool_tsm.1.*
%{_mandir}/man1/ltsmc.1.*
%{_bindir}/ltsmc
%{_sbindir}/lhsmtool_tsm
%{_unitdir}/%{name}.lhsmtool_tsm.service
%{_etcdir}/default/lhsmtool_tsm

%post
%systemd_post %{name}.lhsmtool_tsm.service

%preun
%systemd_preun %{name}.lhsmtool_tsm.service

%postun
%systemd_postun %{name}.lhsmtool_tsm.service

%clean
rm -rf %{buildroot}

%changelog

* Mon May 30 2022 Thomas Stibor <t.stibor@gsi.de> 0.8.4-1
- Implement FSQ protocol version 1

* Tue Jan 26 2021 Thomas Stibor <t.stibor@gsi.de> 0.8.3-2
- Improvement: Remove useless mutex lock on llapi_hsm_request

* Thu Jan 14 2021 Thomas Stibor <t.stibor@gsi.de> 0.8.3-1
- Fix: llapi_hsm_action_progress: Cannot allocate memory (12)

* Tue Nov 24 2020 Thomas Stibor <t.stibor@gsi.de> 0.8.2-1
- Autoconf requires Lustre header files not full Lustre sources.
- Add script ltsmsync.sh into package.

* Wed Jan 31 2020 Thomas Stibor <t.stibor@gsi.de> 0.8.1-1
- TSM file system daemon bug fixes.

* Wed Jan 15 2020 Thomas Stibor <t.stibor@gsi.de> 0.8.0-1
- Added Lustre TSM file system daemon.

* Thu Aug 8 2018 Thomas Stibor <t.stibor@gsi.de> 0.7.3-1
- Lower and upper date/time bound for querying.
- Display in query results the CRC32 value.

* Thu Apr 5 2018 Thomas Stibor <t.stibor@gsi.de> 0.7.2-1
- Introduce and conf file option setting and man page update and corrections.

* Mon Dec 11 2017 Thomas Stibor <t.stibor@gsi.de> 0.7.1-1
- Update systemd requirements and fix archive_id handling
