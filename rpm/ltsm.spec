%define name ltsm
%define version %{_version}
%define release %{_release}

Summary: Lustre TSM copytool and TSM console client.
Name: %{name}
Version: %{version}
Release: %{release}
Source0: rpm/SOURCES/%{name}-%{version}-%{release}.tar.gz
License: GPLv2
BuildRoot: %{_tmppath}/%{name}-buildroot
BuildArch: x86_64
Vendor: GSI
Packager: Thomas Stibor
Requires: TIVsm-API64 >= 7
Url: http://github.com/tstibor/ltsm

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
make install DESTDIR=%{buildroot}

%files
%defattr(-,root,root)
%{_mandir}/man1/lhsmtool_tsm.1.*
%{_mandir}/man1/ltsmc.1.*
%{_bindir}/ltsmc
%{_sbindir}/lhsmtool_tsm

%clean
rm -rf %{buildroot}
