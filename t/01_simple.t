use strict;
use warnings;
use Test::More;
use DBI;
use DBIx::Shinko;

my $dbh = DBI->connect('dbi:SQLite:');
$dbh->do(q{create table cd (cd_id integer primary key, title)});
my $db = DBIx::Shinko->new(dbh => $dbh);
$db->insert(cd => {title => 'awawa'});
my $cd = $db->single(cd => ['*'], {title => 'awawa'});
is $cd->title, 'awawa';
$cd->title('oyoyo');
is $cd->title, 'oyoyo';
$cd->update();
my $cd_id = $cd->cd_id;
{
    my $cd = $db->single(cd => ['*'], {cd_id => $cd_id});
    is $cd->title, 'oyoyo';
}
$cd->delete();

done_testing;

