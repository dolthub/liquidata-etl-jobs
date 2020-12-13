#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper::Simple;
use JSON;

# Schema of wikipedia table and example row:

# |- style="vertical-align:bottom"
# ! District
# ! Member
# ! colspan=2 | Party
# ! Prior experience
# ! Education
# ! Assumed office
# ! Residence
# ! Born

# |-
# | {{ushr|AL|1|X}}
# | data-sort-value="Byrne, Bradley" | [[File:Rep Bradley Byrne (cropped).jpg|75px]]<br/>'''[[Bradley Byrne]]'''
# | style="background-color:{{Republican Party (US)/meta/color}}" |
# | Republican
# | [[Alabama Senate]]<br/>[[Alabama State Board of Education]]
# | [[Duke University]] {{small|([[Bachelor of Arts|BA]])}}<br/>[[University of Alabama]] {{small|([[Juris Doctor|JD]])}}
# | 2014 {{Small|(Special)}}
# | [[Fairhope, Alabama|Fairhope]]
# | 1955

my @members;
my %curr_member;

my $field = 0;

while (<>) {
#    print;
    chomp;
    if ( m/^\|-$/ ) {
        $field = 0;
        unless ( keys %curr_member > 0 ) {
            $field = 1;
            next;
        }
#        print "pushing " . Dumper(%curr_member);
        my %member = %curr_member;
        push @members, \%member;
        %curr_member = ();
    } elsif ( $field == 1 &&  m/^\| \{\{ushr\|([A-Z]{2})\|(.+?)\|X/ ) {
        $curr_member{state} = $1;
        $curr_member{district} = $2;
        #    } elsif ( $field == 2 && m/'''\[\[([^\|\(]*)\]\]'''/ ) {
    } elsif ( $field == 2 && m/data-sort-value="(.*?)"/ ) {
        my ( $last, $first ) = split /,/, $1;
        $curr_member{last_name} = $last;
        $curr_member{first_name} = $first;
    } elsif ( $field == 4 && m/^\| (.*)$/ ) {
        $curr_member{party} = $1;
    } elsif ( $field == 7 && m/(\d{4})/ ) {
        $curr_member{year_assumed_office} = $1;
    } elsif ( $field == 8 && m/\[\[([^\|\]]*)/ ) {
        $curr_member{residence} = $1;
    } elsif ( $field == 9 && m/(\d{4})/ ) {
        $curr_member{birth_year} = $1;
    } elsif ( $field == 10 && m/\| ([MF])/ ) {
        $curr_member{sex} = $1;
    }

    $field++;
}

my %member = %curr_member;
push @members, \%member;

my @filtered = grep { keys %$_ > 2 } @members;
my @missing = grep { keys %$_ <= 2 } @members;

# foreach my $m ( @missing ) {
#     print Dumper($m);
# }

my %json = ( 'rows' => \@filtered );

print encode_json(\%json);

# foreach my $m ( @filtered ) {
#     print Dumper($m);
# }

