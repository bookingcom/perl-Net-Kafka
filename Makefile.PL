use 5.22.0;
use strict;
use warnings;

use Config;
use ExtUtils::MakeMaker;
use ExtUtils::PkgConfig;

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

my %makemaker_args = (
    LIBS => ['-lpthread -lrdkafka'],
);

# If you have a local librdkafka installation and you want to use that instead
# of the system-wide, do something like:
# PKG_CONFIG_PATH=/usr/local/lib/pkgconfig perl Makefile.PL
# or whatever is the actual installation path
my %rdkafka;
eval {
    %rdkafka = ExtUtils::PkgConfig->find('rdkafka');
    1;
} or do {
    my $pkg_config_error = $@ || 'Zombie error';

    eval {
        require Alien::librdkafka;
        my $dist_dir = Alien::librdkafka->dist_dir;
        local $ENV{PKG_CONFIG_PATH} = $ENV{PKG_CONFIG_PATH} // '';
        # TODO: broken for Windows
        $ENV{PKG_CONFIG_PATH} .= ':' . File::Spec->catdir($dist_dir, qw<lib pkgconfig>);
        # Try again, first for the static version (since linking to the
        # shared library when it comes from some Perl package will make it
        # unreachable at runtime)
        eval {
            %rdkafka = ExtUtils::PkgConfig->find('rdkafka-static');
            $makemaker_args{LDDLFLAGS} = join ' ',
                $Config{lddlflags},
                $rdkafka{libs},
            ;
            $makemaker_args{LIBS} = ['-lpthread'];
            1;
        } or do {
            %rdkafka = ExtUtils::PkgConfig->find('rdkafka');
        };
        1;
    } or do {
        my $alien_librdkafka_error = $@ || 'Zombie error';
        die "pkg-config nor Alien::librdkafka could find kafka; bailing out!\n"
           ."PkgConfig error: $pkg_config_error\n"
           ."Alien::librdkafka error: $alien_librdkafka_error\n"
        ;
    };
};

$makemaker_args{INC} = $rdkafka{cflags};

warn sprintf(
    "WARNING: Installed librdkafka version %s is lower than tested %s",
    $rdkafka{modversion},
    LIBRDKAFKA_MIN_VERSION
) if $rdkafka{modversion} lt LIBRDKAFKA_MIN_VERSION;

print "Compiling Net::Kafka with librdkafka $rdkafka{modversion}\n\n";

WriteMakefile(
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
    %makemaker_args,
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
