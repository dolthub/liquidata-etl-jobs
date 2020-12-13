#!/usr/bin/perl

use warnings;
use strict;

use Parse::CSV;
use Getopt::Long;

my $spread = 0.5;
GetOptions ("spread|s=f" => \$spread) or die ("Error in command line args");

open(my $fh, "<-:encoding(UTF-8)") or die $!;

my $num_sims = 10000;

my $csv = Parse::CSV->new(
    handle => $fh,
    names  => 1,
    );

my @reps;
while ( my $line = $csv->fetch ) {
    $line->{losses} = 0;
    push @reps, $line;
}

my %losses = (
    'Republican' => [],
    'Democratic' => [],
    'Independent' => [],
    );

foreach my $i ( 1..$num_sims ) {
    my @losses;
    foreach my $rep ( @reps ) {
        # Roll the dice twice: once to see if they contract COVID-19, and again to determine their outcome
        if ( rand() < $spread && rand() < $rep->{mortality_rate} ) {
            $rep->{losses} += 1;
            push @losses, $rep;
        }
    }

    while ( my ($party, $loss_count) = each %losses ) {
        my @party_losses = grep { $_->{party} eq $party } @losses;
        push @$loss_count, scalar(@party_losses);
    }
}

while ( my ($party, $loss_count) = each %losses ) {
    my @sorted_losses = sort @$loss_count;
    my $median_losses = $sorted_losses[$num_sims / 2];
    print "$party party lost a median $median_losses representatives\n";
}
