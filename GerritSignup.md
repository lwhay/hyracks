# Signing Up for Gerrit #

You should only need to perform the following steps once.

Our Gerrit server is here: https://asterix-gerrit.ics.uci.edu/

  1. Visit the above URL, and click on "Register" in the upper right.
  1. Gerrit uses OpenID; you may create an account using any OpenID provider, or with a Yahoo! account.
  1. Click on "Sign in with a Yahoo! ID". You will be taken to a page to log in to Yahoo!.
  1. From there, you will get a page asking you to allow asterix-gerrit.ics.uci.edu to access your email and basic information.  Approve this request.
  1. You will be directed back to Gerrit to complete the account signup. Make sure to fill out this page! It is necessary to allow you to push code changes to Gerrit for review.
  1. For "Full Name" and "Preferred Email", enter what you like. If you would like to register a different email address such as an @uci.edu address, you may do so. This is where Gerrit will send you notifications about code review requests, voting, and so on.
  1. For "Select a unique username", choose a short alphanumeric ID. This will be your ssh username for git access. Be sure to click "Select Username" to verify that the name is unique and allowed.
  1. Also provide a ssh public key here. There is information on the page on creating one if you do not have one. Click "Add" when done.
    * Note that you can create a new ssh public key just for Gerrit if you like; if you do so you will need to modify your .ssh/config file to ensure the right key is selected.
  1. Click "Continue" at the bottom of the page.
  1. Finally, verify that you have everything set up: From a command-line, type
```
ssh -p 29418 username@asterix-gerrit.ics.uci.edu
```
  1. You should see
```
  ****    Welcome to Gerrit Code Review    ****

  Hi Full Name, you have successfully connected over SSH.

  Unfortunately, interactive shells are disabled.
  To clone a hosted Git repository, use:

  git clone ssh://username@asterix-gerrit.ics.uci.edu:29418/REPOSITORY_NAME.git

```
  1. At this point, please send an email to Chris or Ian, specifying the email address of your new account, and ask to be added to the "AsterixDB Devs" group. You may push change proposals to Gerrit, and vote +1/-1 Code Review on any proposals without being in AsterixDB Devs, but you cannot vote +2 on anything to allow a change to be merged.