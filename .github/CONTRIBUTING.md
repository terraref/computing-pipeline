# How to contribute

Third-party contributions are highly encouraged. 
Our goal is to keep it as easy as possible for you contribute changes that get things working in your environment. 
There are a few guidelines that we need contributors to follow so that we can have a chance of keeping on top of things.

There are many ways to get in touch:

* report bugs
* request features
  * support for existing databases / APIs
  * integrating existing algorithms into the pipeline
* join discussion of open issues, features, and bugs
  * computing pipeline: github.com/terraref/computing-pipeline/issues
  * reference data: github.com/terraref/reference-data/issues
* contribute algorithms to the pipeline
* revise documentation

New features are directed toward developing and extending the TERRA Ref computational infrastructure. 

If you need any help, please contact us in our [chat room](https://gitter.im/terraref/reference-data) or creating a [new issue](https://github.com/terraref/computing-pipeline/issues/new).

## Related Projects

This pipeline combines a number of related projects. 
In many cases, new features may be discussed in this repository but eventually added directly to existing software. 
In these cases, please link the issues across repositories (e.g. add `will be implemented by pecanproject/bety#123` to the issue.

Some of the related software we will be using and contributing to.

* BETYdb: github.com/pecanproject/bety
* CoGe (genomics pipeline): https://github.com/LyonsLab/coge
* NCO (raster data processing): https://github.com/nco/nco
* Clowder: https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/clowder/browse
* Breeding Management Systems
* PlantCV (image processing): https://github.com/danforthcenter/plantcv

## Creating Issues

- Make sure you have a GitHub account.
- Search GitHub and Google to see if your issue has already been reported
        - Create an issue in GitHub, assuming one does not already exist.
	- Clearly describe the issue including steps to reproduce when it is a bug.
	- Make sure you fill in the earliest version that you know has the issue.
- Ask @dlebauer or @robkooper to add you to the TERRA Ref project if you plan on fixing the issue.

* Github Issues
  * [computing pipeline (infrastructure, algorithms)](github.com/terraref/computing-pipeline/issues/new)
  * [reference data (data products)](github.com/terraref/reference-data/issues/new)

## Contributing Text or Code

### Overview

When you add a significant **new feature**, please create an issue first, to allow others to comment and give feedback. 

When you have created a new feature or non-trivial change to existing code, create a 'pull request'.

**Branching and Pull Requests**: Although many core developers have write permissions on the TERRA Ref repositories, 
_please use the feature branch workflow_ (below) in order to allow pull requests, automated testing, and code review.


### Web and Desktop Interfaces

If you haven't used git before, the GitHub website and GitHub desktop client allow you to do all of the following within a graphical user interface. The GitHub interface is often the easiest way to make changes even if you do know git. These make contributing easy and fun, and are well documented.

Any file can be edited in the GitHub interface, and new files can be created. 
GitHub will create these as a new pull request.

### Using Git at the Command Line

Introduce your self to GIT, make sure you use an email associated with your GitHub account.

```
git config --global user.name "John Doe"
git config --global user.email johndoe@example.com
```

[Fork this repository](https://github.com/terraref/computing-pipeline/new/master#fork-destination-box)

Clone your fork of this repository

```
git clone https://github.com/<your username>/computing-pipeline.git
```

Setup repository to be able to fetch from the master

```
git remote add upstream https://github.com/terraref/computing-pipeline.git
```

### Adding Features and Submitting Changes

Always work in a branch rather than directly on the master branch.
Branches should focus on fixing or adding a single feature or set of closely related features 
because this will make it easier to review and merge your contributions.
If more than one person is working on the same code, make sure to keep your master branch in sync with the master of the terraref/computing-pipeline repository. 

Here is a simplified workflow on how add a new feature:

### Get latest version

Update your master (both locally and on GitHub)

```
git fetch upstream
git checkout master
git merge upstream/master
git push
```

### Create a branch to do your work.

A good practice is to call the branch in the form of GH-<issue-number> followed by the title of the issue. This makes it easier to find out the issue you are trying to solve and helps us to understand what is done in the branch. Calling a branch my-work is confusing. Names of branch can not have a space, and should be replaced with a hyphen.

```
git checkout -b GH-issuenumber-title-of-issue
```

### Work and commit

Do you work, and commit as you see fit.Make your commit messages helpful. 

### Push your changes up to GitHub.

If this is the first time pushing to GitHub you will need to extended command, other wise you can simply do a `git push`.

```
git push -u origin GH-issuenumber-title-of-issue
```

### Pull Request

When finished create a pull request from your branch to the main pecan repository.

## Code Of Conduct

### Summary: 

Harassment in code and discussion or violation of physical boundaries is completely unacceptable anywhere in TERRA-REF’s project codebases, issue trackers, chatrooms, mailing lists, meetups, and other events. Violators will be warned by the core team. Repeat violations will result in being blocked or banned by the core team at or before the 3rd violation.

### In detail

Harassment includes offensive verbal comments related to gender identity, gender expression, sexual orientation, disability, physical appearance, body size, race, religion, sexual images, deliberate intimidation, stalking, sustained disruption, and unwelcome sexual attention.

Individuals asked to stop any harassing behavior are expected to comply immediately.

Maintainers are also subject to the anti-harassment policy.

If anyone engages in harassing behavior, including maintainers, we may take appropriate action, up to and including warning the offender, deletion of comments, removal from the project’s codebase and communication systems, and escalation to GitHub support.

If you are being harassed, notice that someone else is being harassed, or have any other concerns, please contact a member of the core team or email dlebauer@illinois.edu immediately.

We expect everyone to follow these rules anywhere in TERRA-REF's project codebases, issue trackers, chatrooms, and mailing lists.

Finally, don't forget that it is human to make mistakes! We all do. Let’s work together to help each other, resolve issues, and learn from the mistakes that we will all inevitably make from time 

### Thanks

Thanks to the [Fedora Code of Conduct](https://getfedora.org/code-of-conduct) and [JSConf Code of Conduct](http://jsconf.com/codeofconduct.html).

