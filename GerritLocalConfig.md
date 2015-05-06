# Configuring your local working environment for Gerrit #

## One-time tasks ##

  1. At the command-line, run the following. Replace "username" with the user name you chose in step 7 above.
```
git config --global gerrit.url ssh://username@asterix-gerrit.ics.uci.edu:29418/
```
  1. Download my "git-gerrit" utility library:
```
git clone https://github.com/ceejatec/git-gerrit
```
  1. Ensure that the `git-gerrit/git-gerrit` script is on your PATH. You could copy it to somewhere, but it would be best to leave it inside the github clone so you can easily get updates.

## Once-per-repository tasks ##

  1. To work on (say) Asterix, first clone the Google Code repository (if you already have a local clone, great!).
```
git clone https://code.google.com/p/asterixdb
```
  1. `cd` into the clone repo directory, and then run the following command to create the "gerrit" remote.
```
git gerrit init
```