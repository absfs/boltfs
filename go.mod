module github.com/absfs/boltfs

go 1.23

require (
	github.com/absfs/absfs v0.0.0-20251208232938-aa0ca30de832
	github.com/absfs/fstesting v0.0.0-20251207022242-d748a85c4a1e
	github.com/absfs/fstools v0.0.0-00010101000000-000000000000
	github.com/absfs/memfs v0.0.0-20251208230836-c6633f45580a
	go.etcd.io/bbolt v1.3.7
)

require (
	github.com/absfs/inode v0.0.0-20251208170702-9db24ab95ae4 // indirect
	golang.org/x/sys v0.4.0 // indirect
)

replace (
	github.com/absfs/absfs => ../absfs
	github.com/absfs/fstesting => ../fstesting
	github.com/absfs/fstools => ../fstools
	github.com/absfs/inode => ../inode
	github.com/absfs/memfs => ../memfs
	github.com/absfs/osfs => ../osfs
)
