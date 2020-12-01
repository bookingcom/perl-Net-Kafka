use 5.22.0;
use strict;
use warnings;

use Config;
use ExtUtils::MakeMaker;
use ExtUtils::PkgConfig;
use Text::ParseWords qw(shellwords);

use constant LIBRDKAFKA_MIN_VERSION => "1.0.0";

# Fix for OSX:
$ENV{PKG_CONFIG_PATH} .= ':/usr/local/opt/openssl/lib/pkgconfig/'
    if $^O eq 'darwin';
# rdkafka.pc has libcrypto as a required dependency, so
# if libcrypto can't be found, pkg-config won't return
# values for rdkafka.
# Problem: in OSX, the default libcrypto lives in a non-default
# location, meaning that without some tweaking like above,
# we never get values for rdkafka.

sub prepare_lddlflags {
    my ($lddlflags) = @_;
    # `-Wl,-z,now` breaks static linking to librdkafka.a, and
    # the perl you get from pacman in ArchLinux has that by default
    # in lddlflags.
    # Similar with --as-needed
    $lddlflags =~ s<,-z[,= ]now><>g;
    $lddlflags =~ s<,--as-needed\b><>g;
    $lddlflags =~ s<-Wl\s>< >g; # did we remove every linker option?
    return $lddlflags;
}

my @archive_after;
my $makemaker_libs = [];
my $makemaker_inc  = '';

my $ccflags    = $Config{'ccflags'} // '';
my $lddlflags  = prepare_lddlflags($Config{'lddlflags'} // '');

# If you have a local librdkafka installation and you want to use that instead
# of the system-wide, do something like:
# PKG_CONFIG_PATH=/usr/local/lib/pkgconfig perl Makefile.PL
# or whatever is the actual installation path
eval {
    my %rdkafka = ExtUtils::PkgConfig->find('rdkafka');
    $makemaker_libs = '-lpthread -lrdkafka';
    $lddlflags    .= ' ' . $rdkafka{libs};
    $makemaker_inc = $rdkafka{cflags};
    warn sprintf(
        "WARNING: Installed librdkafka version %s is lower than tested %s",
        $rdkafka{modversion},
        LIBRDKAFKA_MIN_VERSION
    ) if $rdkafka{modversion} lt LIBRDKAFKA_MIN_VERSION;

    print "Compiling Net::Kafka with librdkafka $rdkafka{modversion}\n\n";

    1;
} or do {
    my $pkg_config_error = $@ || 'Zombie error';

    eval {
        require Alien::librdkafka;
        $makemaker_inc .= ' ' . Alien::librdkafka->cflags;
        my $libs = Alien::librdkafka->install_type eq 'share'
             ? Alien::librdkafka->libs_static || Alien::librdkafka->libs
             : Alien::librdkafka->libs
        ;
        # cmake/OSX bug: -lz gets transformed into
        #   -l/abs/path/to/libz.tbd
        # in systems with Apple's "built-in dynamic linker cache"
        $libs =~ s<\B-l/></>g;

        my $lib_ext = $Config{lib_ext};
        foreach my $maybe_lib ( shellwords($libs) ) {
            if ( $maybe_lib =~ m/\B-l(\S+)/ ) {
                # -lfoo
                push @$makemaker_libs, $maybe_lib;
            }
            elsif ( $maybe_lib =~ m<-L(\S+)> ) {
                my $lib_dir = $1;
                # /path/to/libs
                # add '-Wl,-rpath=$dir'
                $libs = '-Wl,-rpath,' . $lib_dir . ' ' . $libs;
            }
            elsif ( $maybe_lib =~ m<^/.+$lib_ext$> ) {
                # .a file in pkg-config --libs output -- this needs to come after the -o
                push @archive_after, $maybe_lib;
            }
        }

        $lddlflags .= ' ' . $libs;

        1;
    } or do {
        my $alien_librdkafka_error = $@ || 'Zombie error';
        die "pkg-config nor Alien::librdkafka could find kafka; bailing out!\n"
           ."PkgConfig error: $pkg_config_error\n"
           ."Alien::librdkafka error: $alien_librdkafka_error\n"
        ;
    };
};


WriteMakefile(
    INC          => $makemaker_inc,
    LIBS         => $makemaker_libs,
    LDDLFLAGS    => $lddlflags,
    CCFLAGS      => $ccflags,
    PERL_ARCHIVE_AFTER => join(' ', @archive_after),
    NAME          => 'Net::Kafka',
    DISTNAME      => 'Net-Kafka',
    AUTHOR        => [
        'Rajesh Amradi <rajesh.amradi@booking.com>',
        'Michael Austin <michael.austin@booking.com>',
        'Ankit Bhatnagar <ankit.bhatnagar@booking.com>',
        'Jaap Eldering <jaap.eldering@booking.com>',
        'Osama Elsayed <osama.elsayed@booking.com>',
        'Eduardo Dalla Favera <eduardo.dallafavera@booking.com>',
        'Pavel Gurkov <pavel.gurkov@booking.com>',
        'Przemyslaw Iskra <sparky@pld-linux.org>',
        'Alex Mironov <alex.mironov@booking.com>',
        'Pavel Shaydo <zwon@cpan.org>',
    ],
    LICENSE       => 'perl',
    VERSION_FROM  => 'lib/Net/Kafka.pm',
    ABSTRACT_FROM => 'lib/Net/Kafka.pm',
    OBJECT        => '$(O_FILES)',
    TYPEMAPS      => ['typemap'],
    PREREQ_PM           => {
        'AnyEvent'               => 0,
        'Test::More'             => 0,
        'AnyEvent::XSPromises'   => 0,
    },
    META_MERGE    => {
        "meta-spec" => { version => 2 },
        prereqs   => {
            configure => {
                # optionally use Alien::librdkafka
                recommends => {
                    'Alien::librdkafka' => '0.01',
                },
            },
        },
        resources => {
            homepage    => 'https://github.com/bookingcom/perl-Net-Kafka',
            bugtracker  => {
                web => 'https://github.com/bookingcom/perl-Net-Kafka/issues'
            },
            repository => {
                type    => 'git',
                url     => 'https://github.com/bookingcom/perl-Net-Kafka.git',
                web     => 'https://github.com/bookingcom/perl-Net-Kafka',
            },
        },
    },
    MIN_PERL_VERSION    => '5.22.0',
    CONFIGURE_REQUIRES  => {
        'ExtUtils::MakeMaker' => 0,
        'ExtUtils::PkgConfig' => 0,
    },
);
