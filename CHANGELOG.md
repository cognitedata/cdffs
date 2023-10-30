# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

# Changelog

## [0.1.0] - 2022-12-12

### Added

- Initial version of cognite-cdffs scripts.
- This CHANGELOG file to hopefully serve as an evolving example of a
  standardized open source project CHANGELOG.

## [0.1.1] - 2022-12-15

### Added
- Adding twine as dev dependency
- Workflow updates for code coverage

### Fixed
- Fixing dependency issues & minor release issues


## [0.1.2] - 2023-02-15

### Fixed
- Bug fixes on file sizes.
- pre-commit hook updates
- Updates based on new black version

## [0.1.3] - 2023-02-16

### Fixed
- dependency version updates
- Syncing pyproject.toml & poetry.lock


## [0.1.4] - 2023-03-13

### Added
- Updates to handle file metadata for each file when they are opened.
- Integration test updates for File IO operations.
- Documentation updates

### Fixed
- Poetry config file updates
- dependency version upgrades
- Fixed doc-strings

## [0.2.0] - 2023-04-04

### Added
- Authentication to CDF is part of `cdffs`.
- Documentation updates.

### Fixed
- dependency version upgrades
- Poetry version upgrade


## [0.2.1] - 2023-04-05

### Fixed
- Bug fixes on documentation.
- Bug fixes on Authentication.
- Dependency updates for docs
- Poetry version upgrade

[unreleased]:

## [0.2.2] - 2023-05-22

### Fixed
- Dependency updates for docs & source code.

[unreleased]:

## [0.2.3] - 2023-05-25

### Fixed
- Dependency updates for docs & source code.
- Security fixes for requests package.

[unreleased]:

## [0.2.4] - 2023-06-13

### Fixed
- Added `exists` method to check if a file exists in CDF.
- Added `rm_files` method to delete a list of files from CDF.
- Added capability to allow users to configure download retry parameters.
- Dependency updates for docs & source code.

[unreleased]:


## [0.2.5] - 2023-06-22

### Fixed
- Added `limit` option for `ls` method. It will be useful for file io operations.
- Dependency updates for docs & source code.

[unreleased]:


## [0.2.6] - 2023-07-03

### Fixed
- Directory cache is updated to have latest file size information when the file is re-uploaded immediately.
- Dependency updates for docs & source code.

[unreleased]:


## [0.2.7] - 2023-08-01

### Fixed
- Dependency updates for docs & source code to eliminate the security vulnerabilities found on the `cryptography` and `certifi` packages.

[unreleased]:

## [0.2.8] - 2023-09-29

### Fixed
- Dependency updates for docs & source code.

[unreleased]:

## [0.2.9] - 2023-10-26

### Added
- Support for native multipart file upload for CDF in Azure and Google. 

## [0.2.10] - 2023-11-1

### Fixed
- Fix internal cache accumulated if big files are handled with native multipart implementation 
