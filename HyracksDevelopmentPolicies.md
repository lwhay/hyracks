# Policies and processes #

This wiki page describes the policies that apply to Hyracks development.

## Source code layout in SVN ##

  * /svn/trunk : Contains the latest stable source code of the Hyracks project
  * /svn/tags : Contains released versions of Hyracks
  * /svn/branches : Contains development versions of Hyracks

## Development process ##

Direct checkin of source code into /svn/trunk is discouraged. Adding a new feature into Hyracks entails creation of a development branch in /svn/branches. In order to make sure that this is done in a controlled and manageable manner, certain processes must be followed.

### Creation of a feature branch ###

  1. Create an Issue in the Issue tracking system describing the feature for which the branch is requested with a suggested name for the branch. The Issue must identify at least one owner for the branch who is responsible for the contents of the branch. Assign the Issue to any Branches Manager listed in BranchManagement
  1. Once the branch is created by the Branches Manager, you may start development on the feature in the created branch.
  1. Update the BranchManagement page to contain information about the branch for tracking purposes.

### Merging of a feature branch into trunk ###

  1. At some point when the owner(s) of the development branch are done with the feature under development, it is time to merge the branch into trunk. It is the owners' responsibility to make sure that sufficient tests are added to the code base to verify that the newly added feature works.
  1. Merge all changes that occurred subsequent to the creation of the development branch from /svn/trunk into the development branch resolving all conflicts.
  1. Make sure Hyracks builds successfully and passes all tests.
  1. Before merging the development branch into trunk, request a code review by the owner(s) of /svn/trunk listed in BranchManagement.
  1. Once the review process terminates, the owner(s) of /svn/trunk will perform the merge into trunk.

## Coding Conventions ##

### Code Formatting ###

  1. Every source file in the codebase MUST contain the standard Copyright header containing the Apache Software License information
  1. Every source file MUST be formatted using the Eclipse formatting template attached at http://hyracks.googlecode.com/files/HyracksCodeFormatProfile.xml