# Verification #

As of right now, whenever a patch set is submitted to Gerrit (i.e. whenever a user performs 'git gerrit submit') are automatically picked up by Jenkins, which runs `mvn verify` to test the patch fully. Once the build finishes, Jenkins comments on the patchset and votes with the result (+1 for all tests passing, -1 for anything less). Ideally, and in most cases, verifocation of a patchset reqires no intervention. However there are a few exceptions to this as detailed below.

## Retrigggering builds and triggering builds manually ##

### Retriggering ###
Occasionally, there are builds in which tests failed, but perhaps not for a reason that has anything to do with the proposed patch's changes. For example, on occasion the Integration tests can have issues with ports on the build server already being bound. The simple work around to this is to try building again.

One way to perform this is by simply visiting the link that is posted on Gerrit by Jenkins, and hitting the 'Retrigger' link on the left-hand side of the page. This will try retesting the build with the exact same parameters as last time, hopefully with a different result.

### Manual Trigger ###
Builds of Gerrit patches can also be fully manually triggered from Jenkins. This can be desirable if a change is now dependent on a Hyracks change (via the Topic field feature described below), or if the patch is a draft or is not triggered automatically for any other reason. To manually trigger a build of a patchset, visit the Jenkins front-page, and click the 'Query and Trigger Gerrit patches' link. This should link to a page allowing you to manually trigger Gerrit patches. In the search field any query that can be searched in the Gerrit web interface can be used, but the default query of all open patchsets is likely fine. Hit the 'Search' button, and then click the checkbox that represents the patch that needs to be built. Then simply click 'Trigger Selected' and the patch will start building.

## Cross-project Dependencies ##

It is sometimes the case that a change in one related project alters interfaces or expected results that an upstream project relies on. The converse can be true as well, such as an AsterixDB change that needs a Hyracks feature. In these sorts of situations, there is a mechanism to describe to Jenkins this dependenc of one project's patchset to a particular Hyracks Change, by using the 'Topic' field in Gerrit. To utilize
this feature, perform the following steps:

  1. First submit the Hyracks change which the upstream (AsterixDB,etc..) change depends on if you have not done so already
  1. In the Hyracks change details on the Gerrit web interface, take note of the refspec in the _Download_ field of the patchset details. It will look something like `refs/changes/07/207/1`. This is what uniquely identifies the Hyracks change in Gerrit.
  1. Submit the dependent change now, if you have not done so already. In the Gerrit web interface, view the change's details. In the upper left-hand corner, there will be an empty field in a table, labeled `Topic`, with a notepad icon right-justified in the second column of the table. Click the notepad Icon, and in the resulting dialog box, enter in the refspec (`refs/changes/07/207/1`) as the Topic value.
  1. Now, manually trigger the dependent change in Jenkins (see previous section for details)
