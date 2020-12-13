#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper::Simple;
use Date::Parse;
use Date::Simple;
use JSON;

# Schema of wikipedia table and example row:

# |-
# ! State
# ! Image
# ! Senator
# ! colspan=2 | Party
# ! Born
# ! Occupation(s)
# ! Previous<br/>office(s)
# ! Assumed office
# ! {{nowrap|Term up}}
# ! Residence

# |-
# | rowspan=2 | [[List of United States senators from Alabama|Alabama]]
# | [[File:Richard Shelby, official portrait, 112th Congress (cropped).jpg|95px]]
# ! {{sortname|Richard|Shelby}}
# | style="background-color:{{Republican Party (US)/meta/color}}" |
# | [[Republican Party (United States)|Republican]]
# | {{birth date and age|1934|5|6}}
# | Lawyer
# | {{ushr|AL|7|U.S. House}}<br/>[[Alabama Senate]]
# | data-sort-value="1987/01/03" | January 3, 1987
# | [[2022 United States Senate election in Alabama|2022]]
# | [[Tuscaloosa, Alabama|Tuscaloosa]]<ref name="senate.gov">{{cite web | url=http://www.senate.gov/states/AL/intro.htm | title=States in the Senate – AL Introduction | website=Senate.gov | accessdate=January 3, 2019}}</ref>

my @members;
my %curr_member;

my $field = 0;
my $state;

while (<>) {
    chomp;
#    print and print " $field\n";
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
    } elsif ( $field == 1 &&  m/rowspan=2.*from ([A-Za-z ]+)/ ) {
        $state = $1;
        $field = 0;
    } elsif ( $field == 2 &&  m/sortname\|(.+?)\|(.+?)[^A-Za-z]/ ) {
        $curr_member{last_name} = $2;
        $curr_member{first_name} = $1;
        $curr_member{state} = $state;
    } elsif ( $field == 4 && m/(Republican|Democratic|Independent)/ ) {
        $curr_member{party} = $1;
    } elsif ( $field == 5 && m/birth date and age\|(\d{4})\|(\d+)\|(\d+)/ ) {
        $curr_member{birth_date} = "" . Date::Simple->new($1, $2, $3);
    } elsif ( $field == 8 && m/data-sort-value="\d{4}.*?\|\s*([A-za-z0-9 ,]+)/ ) {
        my ($ss,$mm,$hh,$day,$month,$year,$zone) = strptime($1);
        my $d = Date::Simple->new($year + 1900, $month + 1, $day);
        if ( !$d ) {
            print and print " 1 is $1\n";
        }
        $curr_member{date_assumed_office} = "$d";
    } elsif ( $field == 10 && m/\[\[([^\|\]]*)/ ) {
        $curr_member{residence} = $1;
    } elsif ( $field == 11 && m/\| ([MF])/ ) {
        $curr_member{sex} = $1;
    }

    $field++;
}

my %member = %curr_member;
push @members, \%member;

my @filtered = grep { keys %$_ > 2 } @members;
my @missing = grep { keys %$_ <= 2 } @members;

# foreach my $m ( @members ) {
#     print Dumper($m);
# }

my %json = ( 'rows' => \@filtered );
print encode_json(\%json);

# foreach my $m ( @filtered ) {
#     print Dumper($m);
# }

