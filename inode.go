package boltfs

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	bolt "github.com/coreos/bbolt"
)

var errNilNode = errors.New("nil node")

// An iNode represents the basic metadata of a file.
type iNode struct {
	Ino   uint64
	Mode  os.FileMode
	Nlink uint64
	Size  int64

	Ctime time.Time // creation time
	Atime time.Time // access time
	Mtime time.Time // modification time

	Uid uint32
	Gid uint32

	Children entries
}

// os.FileInfo implementation
type inodeinfo struct {
	name string
	node *iNode
}

func (info inodeinfo) Name() string {
	return info.name
}

func (info inodeinfo) IsDir() bool {
	return info.node.IsDir()
}

func (info inodeinfo) Mode() os.FileMode {
	return info.node.Mode
}

func (info inodeinfo) ModTime() time.Time {
	return info.node.Mtime
}

func (info inodeinfo) Size() int64 {
	return info.node.Size
}

func (info inodeinfo) Sys() interface{} {
	return info.node
}

func i2b(i uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	return b[:]
}

func b2i(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

type entry struct {
	Name string
	Ino  uint64
}

func (e *entry) String() string {
	return fmt.Sprintf("%q:%d", e.Name, e.Ino)
}

type entries []*entry

func (e entries) String() string {
	var out []string
	for _, entry := range e {
		out = append(out, entry.String())
	}
	return fmt.Sprintf("entries[%s]", strings.Join(out, ","))
}

func (e entries) Len() int           { return len(e) }
func (e entries) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e entries) Less(i, j int) bool { return e[i].Name < e[j].Name }

func newInode(mode os.FileMode) *iNode {
	now := time.Now()
	return &iNode{
		Atime: now,
		Mtime: now,
		Ctime: now,
		Mode:  mode,
	}
}

func copyInode(source *iNode) *iNode {
	target := &iNode{
		Ino:   source.Ino,
		Mode:  source.Mode,
		Nlink: source.Nlink,
		Size:  source.Size,

		Ctime:    source.Ctime,
		Atime:    source.Atime,
		Mtime:    source.Mtime,
		Uid:      source.Uid,
		Gid:      source.Gid,
		Children: make(entries, len(source.Children)),
	}

	for i := range source.Children {
		target.Children[i] = &entry{
			Name: source.Children[i].Name,
			Ino:  source.Children[i].Ino,
		}
	}

	return target
}

// Adds a child to the node with the given `name` and `ino`.
// If there is a child that already has that name present, it
// is replaced and it's ino is returned.
func (n *iNode) Link(name string, ino uint64) (uint64, error) {
	if !n.IsDir() {
		return 0, errNotDir
	}

	x := sort.Search(len(n.Children), func(i int) bool {
		return n.Children[i].Name >= name
	})

	if x == len(n.Children) || n.Children[x].Name != name {
		n.Children = append(n.Children, nil)
		copy(n.Children[x+1:], n.Children[x:])
		n.Children[x] = &entry{name, 0}
	}

	// swap the ino in the entry for the ino provided (0 if we just created it)
	ino, n.Children[x].Ino = n.Children[x].Ino, ino
	n.modified()
	return ino, nil
}

func (n *iNode) Unlink(name string) (uint64, error) {

	x := sort.Search(len(n.Children), func(i int) bool {
		return n.Children[i].Name >= name
	})
	if x == len(n.Children) || n.Children[x].Name != name {
		return 0, syscall.ENOENT // os.ErrNotExist
	}
	old := n.Children[x].Ino
	copy(n.Children[x:], n.Children[x+1:])
	n.Children = n.Children[:len(n.Children)-1]
	n.modified()
	return old, nil
}

func decodeNode(b *bolt.Bucket, ino uint64, node *iNode) error {
	if b == nil {
		panic("nil bucket")
	}
	if node == nil {
		return errNilNode
	}
	data := b.Get(i2b(ino))
	if data == nil || len(data) == 0 {
		return os.ErrNotExist
	}
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(node)
	if err != nil {
		return err
	}
	return nil
}

func encodeNode(b *bolt.Bucket, ino uint64, node *iNode) error {
	if b == nil {
		panic("nil bucket")
	}
	if node == nil {
		return errNilNode
	}
	w := new(bytes.Buffer)
	err := gob.NewEncoder(w).Encode(node)
	if err != nil {
		return err
	}

	return b.Put(i2b(ino), w.Bytes())
}

func findChild(node *iNode, name string) (uint64, error) {
	x := node.find(name)
	if x == len(node.Children) {
		return 0, os.ErrNotExist
	}
	return node.Children[x].Ino, nil
}

// func (n *iNode) UnlinkAll(fs *FileSystem) {
// 	fs.loadInode(ino)
// 	for _, e := range n.Children {
// 		node, err :=
// 		if e.Ino == n.Ino {
// 			e.node.countDown()
// 			continue
// 		}
// 		e.node.UnlinkAll()
// 		e.node.countDown()
// 	}
// 	n.Children = n.Children[:0]
// }

func (n *iNode) IsDir() bool {
	return os.ModeDir&n.Mode != 0
}

// func (n *iNode) Move(source, target string) error {

// 	dir, name := filepath.Split(source)
// 	dir = filepath.Clean(dir)

// 	snodeIno, err := n.Resolve(source)
// 	if err != nil {
// 		return err
// 	}
// 	snode := snodeIno.Get(n.db)

// 	pIno, err := n.Resolve(dir)
// 	if err != nil {
// 		return err
// 	}

// 	p := pIno.Get(n.db)

// 	var rename string
// 	tnodeIno, err := n.Resolve(target)
// 	var tnode *iNode
// 	if tnodeIno != 0 {
// 		tnode = tnodeIno.Get(n.db)
// 	}
// 	if (err == nil && !tnode.IsDir()) || (err != nil && os.IsNotExist(err)) {
// 		var tdir string
// 		tdir, rename = filepath.Split(target)
// 		tdir = filepath.Clean(tdir)
// 		tnode, err = n.Resolve(tdir)
// 	}
// 	if err != nil {
// 		return err
// 	}

// 	if len(rename) > 0 {
// 		name, rename = rename, name
// 	}
// 	err = tnode.Link(name, snode)
// 	if err != nil {
// 		return err
// 	}
// 	if len(rename) > 0 {
// 		name, rename = rename, name
// 	}
// 	err = p.Unlink(name)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (n *iNode) Resolve(path string) (iNodeID, error) {
// 	name, trim := PopPath(path)
// 	if name == "/" {
// 		if trim == "" {
// 			return n, nil
// 		}
// 		nn, err := n.Resolve(trim)
// 		if err != nil {
// 			return 0, err
// 		}

// 		if nn == 0 {
// 			return n, nil
// 		}
// 		return nn, err
// 	}

// 	x := n.find(name)
// 	if x < len(n.Children) && n.Children[x].Name == name {
// 		nn := n.Children[x].Ino
// 		if len(trim) == 0 {
// 			return nn, nil
// 		}
// 		return nn.Resolve(trim)
// 	}
// 	return nil, syscall.ENOENT // os.ErrNotExist
// }

func (n *iNode) accessed() {
	n.Atime = time.Now()
}

func (n *iNode) modified() {
	now := time.Now()
	n.Atime = now
	n.Mtime = now
}

func (n *iNode) countUp() uint64 {
	n.Nlink++
	n.accessed() // (I don't think link count mod counts as node mod )
	return n.Nlink
}

func (n *iNode) countDown() uint64 {
	if n.Nlink == 0 {
		panic(fmt.Sprintf("inode %d negative link count", n.Ino))
	}
	n.Nlink--
	n.accessed() // (I don't think link count mod counts as node mod )
	return n.Nlink
}

// func (n *iNode) unlinki(i int) {
// 	n.Children[i].iNode.countDown()
// 	copy(n.Children[i:], n.Children[i+1:])
// 	n.Children = n.Children[:len(n.Children)-1]
// 	n.modified()
// }

// func (n *iNode) linkswapi(i int, entry *entry) {
// 	n.Children[i].iNode.countDown()
// 	n.Children[i] = entry
// 	n.Children[i].iNode.countUp()
// 	n.modified()
// }

// func (n *iNode) linki(i int, entry *entry) {
// 	n.Children = append(n.Children, nil)
// 	copy(n.Children[i+1:], n.Children[i:])

// 	n.Children[i] = entry
// 	n.Children[i].iNode.countUp()
// 	n.modified()
// }

func (n *iNode) find(name string) int {
	if !sort.IsSorted(n.Children) {
		sort.Sort(n.Children)
	}

	x := sort.Search(len(n.Children), func(i int) bool {
		return n.Children[i].Name >= name
	})
	if x == len(n.Children) {
		return x
	}
	if n.Children[x].Name != name {
		return len(n.Children)
	}
	return x
}
