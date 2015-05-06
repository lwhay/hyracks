# Making Changes - working method #

  1. When you want to start working on a bug, feature, etc, first make a local `git` branch. Never work directly on `master`! `master` should always be a pure mirror of `origin/master`, ie, Google Code.
```
git checkout -b my_branch
```
  1. Make your changes, test them, etc. Feel free to `git commit` as often as you like.
  1. **Optional**: If you like, you can push your branch up to Google Code, either to share it with others or as a backup. You may do this at whatever point in time you like.
```
git push origin my_branch
```
  1. Every so often, you should update your local `master` mirror, and then merge that onto your working branch. This will prevent your branch from falling too far out of date, and ensure that your code review proposals will merge successfully with `master`. There are a number of ways to do this, but `git-gerrit` provides a convenience function:
```
git gerrit update
```
  1. When you are ready to submit changes for code review, first ensure that you have committed everything locally that is necessary (`git status` should report "nothing to commit, working directory clean"). This is also a good time to update (see step 4). Then run:
```
git gerrit submit
```
  1. This will pop open your editor to invite you to create a good commit message. This will be the single commit message which will be the only one to appear in the project's master git history. Take the time to make it clear. The editor will contain the log messages of everything you committed on your branch as a reminder, but generally you will want to delete all this and replace it with a comprehensive message. Also: As noted in the initial message, the last line of the buffer will contain a `Change-Id` field. Do not delete that line! It is used by Gerrit to identify this particular merge proposal.
  1. When you save your commit message, git-gerrit will push all of the changes from your working branch up to Gerrit. Assuming no errors, you should see output similar to the following:
```
remote: Resolving deltas: 100% (1/1)
remote: Processing changes: new: 1, refs: 1, done    
remote: 
remote: New Changes:
remote:   http://fulliautomatix.ics.uci.edu:8443/30
remote: 
To ssh://ceej@fulliautomatix.ics.uci.edu:29418/ceej-gerrit-test
 * [new branch]      HEAD -> refs/for/master
```
  1. That URL under "New Changes" is your code review! Send it to others to request reviews.
  1. If you get any negative code reviews and need to make changes, you can just repeat steps 2 - 6 of the working method. Your local branch will still have all the history you put there, if you need to revert changes or look back and see what you did, etc.
  1. When you repeat step 6, you will notice two things: First, git-gerrit keeps the change message for you, including the Change-Id, so you don't have to re-invent it every time. Second, the output from `git gerrit submit` will not include the URL of the review this time. I'm not sure why; I wish it did. But if you re-visit the old URL in your browser, you should see an additional "Patch Set" containing your revised changes for people to review.

# Making More Changes #

You may have as many feature branches as you want locally, and each may be at any point in the review cycle.

Once you are done with a change (that is, it has been Code Reviewed and approved in Gerrit and merged), it is probably best to start work on the next big thing on a new branch. However, git-gerrit attempts to be smart if you choose to re-use a branch. For example, it should notice if the "current" Change-Id for a branch has been merged to master, and it will create a new Change-Id and a new fresh commit message the next time you run `git gerrit submit`.

In that case, you may notice that the list of changes on the branch still contains older commits which have been merged. That is due to the unfortunate way Gerrit works; it requires squashing to a single git commit for review, which loses the association with the original commits in your history.

It is also possible that you may want to re-use a branch and git-gerrit fails to notice that the older change has been merged - or perhaps the change was Abandoned on Gerrit and not merged, and you want to continue working on the same branch to create a new proposal. In this case, run
```
git gerrit new
```
on your branch. This will force git-gerrit to create a new Change-Id and commit message the next time you run `git gerrit submit`.