
# BoltFS - A Complete Filesystem Implementation for BoltDB
BoltFS provides a full featured filesystem on top of a boltdb database. This
package implements most of the filesystem functions from the `os` standard
library package, even including support for symbolic links.

## Features

- Compatible with the abstract filesystem interface `absfs.SymlinkFileSystem`
- Support for hard and soft linking
- Walk method like `filepath.Walk`
- Extensive tests

## Coming soon

- In ram thread safe inode cache for performance
- Improved test coverage
- Error for error match to `os` package implementations
- User provided *boltdb.DB support, with bucket isolation
- FastWalk high performance walker (non sorted, os.FileMode only)
- Support for storing file content externally

Also I may add a Fuse interface implementation if there is interest.

## License

MIT license. See LICENSE file for more information.

