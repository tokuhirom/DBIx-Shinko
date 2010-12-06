use strict;
use warnings;
use Test::More;
use DBI;
use DBIx::Shinko;

my $dbh = DBI->connect('dbi:SQLite:');
$dbh->do(q{create table cd (title)});
my $db = DBIx::Shinko->new(dbh => $dbh);
$db->insert(cd => {title => 'awawa'});
my $cd = $db->single(cd => ['*'], {title => 'awawa'});
is $cd->title, 'awawa';

done_testing;

