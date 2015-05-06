# Introduction #

Gerrit is the tool which allows us to coordinate and centralize code review and testing for development on both the Hyracks and Asterix projects. Once
a change is pushed via git, it will appear in the open reviews list. From there the review and verification process is started. Once a change has a patch set with a +2 Code Review, +1 Verified, and no -2 Code Reviews or -1 Verified marks, it can be merged into the master branch by one of the administrators (Currently Chris Hillery or Ian Maxon) .


# Code Review #

Code reviews are accomplished via in-line comments and a general rating system, similar to how they are in Google Code and Rietveld. However the ratings in Gerrit have a more important meaning than they do in these other review systems. There are 5 ratings for Code Reviews in Gerrit, their  approximate meanings are summarized below:

| +2 | I have reviewed this thoroughly and it meets all standards- it is ready to be merged. |
|:---|:--------------------------------------------------------------------------------------|
| +1 | I have reviewed this change and I think it is good, but I don't want to give the final OK just yet. |
| 0 | No review/No opinion |
| -1 | I think this patch needs some work before it can be merged. |
| -2 | This shouldn't be merged. |

All patchsets require at least one +2 and no -2's to be merged, along with another requirement (+1 Verified) discussed below.

# Verification #

Verification is simply determining that the patch set does not break any test cases in the code. Ideally the verification process requires no intervention at all. Whenever a new patch set is submitted, the build server (Jenkins) will fetch and test it, and report back the results. An all clear results in a vote of +1 Verified, and anything else results in a -1 - which will block merging of the patch set until it is resolved. For further details see UsingJenkinsWithGerrit