package DBIx::Shinko;
use strict;
use warnings;
use 5.008001;
our $VERSION = '0.01';
use DBIx::TransactionManager;
use Class::Accessor::Lite (
    ro => [qw/query_builder dbh row_class/],
);

sub new {
    my $class = shift;
    my %args = @_==1 ? %{$_[0]} : @_;
    my $self = bless {
        row_class => 'DBIx::Shinko::Row',
        %args
    }, $class;
    my $quote_char = $self->dbh->get_info(29) || q{"};
    my $name_sep   = $self->dbh->get_info(41) || q{.};
    $self->{query_builder} ||= SQL::Builder->new(
        driver     => $self->dbh->{Driver}->{Name},
        quote_char => $quote_char,
        name_sep   => $name_sep,
    );
    return $self;
}

sub transaction_manager {
    my $self = shift;
    return $self->{transaction_manager} ||= DBIx::TransactionManager->new($self->dbh);
}

for my $meth (qw/txn_scope txn_begin txn_commit txn_rollback/) {
    no strict 'refs';
    *{__PACKAGE__ . "::${meth}"} = sub {
        my $self = shift;
        $self->transaction_manager->$meth(@_);
    }
}

sub single {
    my ($self, $table, $fields, $where, $opt) = @_;
    my $iter = $self->search($table, $fields, $where, {limit => 1, ($opt ? %$opt : ())});
    return $iter->next;
}

sub search {
    my ($self, $table, $fields, $where, $opt) = @_;
    my ($sql, @bind) = $self->query_builder->select($table, $fields, $where, {limit => 1, %$opt});
    my $sth = $self->dbh->prepare($sql) or Carp::croak($self->dbh->errstr);
    $sth->execute(@bind) or Carp::croak($self->dbh->errstr);
    my $iter = DBIx::Shinko::Iterator->new(sth => $sth, row_class => $self->row_class, dbh => $self->dbh);
    return wantarray ? $iter->all : $iter;
}

sub insert {
    my ($self, $table, $values, $opt) = @_;
    my ($sql, @bind) = $self->query_builder->insert($table, $values, $opt);
    my $sth = $self->dbh->prepare($sql) or Carp::croak($self->dbh->errstr);
    $sth->execute(@bind) or Carp::croak($self->dbh->errstr);
}

sub find_or_create {
    my ( $self, $table, $values ) = @_;
    my $row = $self->single( $table, $values );
    return $row if $row;
    return $self->insert( $table, $values );
}

sub bulk_insert {
    my ( $self, $table, $rows ) = @_;
    my $driver = $self->dbh->{Driver}->{Name};
    if ( $driver eq 'mysql' ) {
        my ( $sql, @binds ) =
          $self->query_builder->insert_multi( $table, $rows );
        $self->dbh->do( $sql, {}, @binds ) or Carp::croak $self->dbh->errstr;
    }
    else {
        for my $row (@$rows) {
            # do not use $self->insert here for consistent behaivour
            my ( $sql, @binds ) = $self->query_builder->insert( $table, $row );
            $self->dbh->do( $sql, {}, @binds )
              or Carp::croak $self->dbh->errstr;
        }
    }
    return;
}

sub delete {
    my ( $self, $table, $where ) = @_;
    my ( $sql, @binds ) = $self->query_builder->delete( $table, $where );
    $self->dbh->do( $sql, {}, @binds ) or Carp::croak($self->dbh->errstr);
}

sub update {
    my ( $self, $table, $attr, $where ) = @_;
    my ( $sql, @binds ) = $self->query_builder->update( $table, $attr, $where );
    $self->dbh->do( $sql, {}, @binds ) or Carp::croak($self->dbh->errstr);
}
                                                    
package DBIx::Shinko::QueryBuilder;
use parent qw/SQL::Builder/;

__PACKAGE__->load_plugin(qw/InsertMulti/);

package DBIx::Shinko::Row;
use Carp ();
use DBIx::Inspector;

sub new {
    my ($class, $data, $dbh) = @_;
    my $self = bless {%$data, __dbh__ => $dbh}, $class;
    $self->mk_accessors(%$data);
    return $self;
}

sub mk_accessors {
    my ($class, @names) = @_;
    $class = ref $class if ref $class;
    no strict 'refs';
    for my $name (@names) {
        *{"$class\::$name"} = sub {
            return $_[0]->get_column($name) if @_==1;
            # return $_[0]->set_column($name => $_[1]) if @_==2;
            Carp::croak("Too much parameters");
        };
    }
}

sub get_column {
    my ($self, $name) = @_;
    Carp::croak("$self does not contains $name") unless exists $self->{$name};
    $self->{$name};
}

package DBIx::Shinko::Iterator;
use Class::Accessor::Lite (
    new => 1,
    ro => [qw/sth row_class dbh/],
);

sub next {
    my $self = shift;
    my $data = $self->sth->fetchrow_hashref();
    if ($data) {
        return $self->row_class->new($data, $self->dbh);
    } else {
        return;
    }
}

sub all {
    my $self = shift;
    my @rows;
    while (my $row = $self->next) {
        push @rows, $row;
    }
    return wantarray ? @rows : \@rows;
}

1;
__END__

=encoding utf8

=head1 NAME

DBIx::Shinko -

=head1 SYNOPSIS

  use DBIx::Shinko;

=head1 DESCRIPTION

DBIx::Shinko is

=head1 AUTHOR

Tokuhiro Matsuno E<lt>tokuhirom AAJKLFJEF GMAIL COME<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright (C) Tokuhiro Matsuno

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
