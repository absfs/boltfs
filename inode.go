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

	bolt "go.etcd.io/bbolt"
)

var errNilNode = errors.New("nil node")

// An iNode represents the basic metadata of a file.
type iNode struct {
	Ino   uint64
	Mode  os.FileMode
	Nlink uint64
	Size  int64

	ctime time.Time // creation time
	atime time.Time // access time
	mtime time.Time // modification time

	Uid uint32
	Gid uint32

	Children entries
}

// Ctime returns the creation time of the inode.
func (n *iNode) Ctime() time.Time { return n.ctime }

// Atime returns the access time of the inode.
func (n *iNode) Atime() time.Time { return n.atime }

// Mtime returns the modification time of the inode.
func (n *iNode) Mtime() time.Time { return n.mtime }

// SetCtime sets the creation time of the inode.
func (n *iNode) SetCtime(t time.Time) { n.ctime = t }

// SetAtime sets the access time of the inode.
func (n *iNode) SetAtime(t time.Time) { n.atime = t }

// SetMtime sets the modification time of the inode.
func (n *iNode) SetMtime(t time.Time) { n.mtime = t }

// gobInode is used for gob encoding/decoding with exported fields
type gobInode struct {
	Ino      uint64
	Mode     os.FileMode
	Nlink    uint64
	Size     int64
	Ctime    time.Time
	Atime    time.Time
	Mtime    time.Time
	Uid      uint32
	Gid      uint32
	Children entries
}

// GobEncode implements gob.GobEncoder
func (n *iNode) GobEncode() ([]byte, error) {
	g := gobInode{
		Ino:      n.Ino,
		Mode:     n.Mode,
		Nlink:    n.Nlink,
		Size:     n.Size,
		Ctime:    n.ctime,
		Atime:    n.atime,
		Mtime:    n.mtime,
		Uid:      n.Uid,
		Gid:      n.Gid,
		Children: n.Children,
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&g)
	return buf.Bytes(), err
}

// GobDecode implements gob.GobDecoder
func (n *iNode) GobDecode(data []byte) error {
	var g gobInode
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&g)
	if err != nil {
		return err
	}
	n.Ino = g.Ino
	n.Mode = g.Mode
	n.Nlink = g.Nlink
	n.Size = g.Size
	n.ctime = g.Ctime
	n.atime = g.Atime
	n.mtime = g.Mtime
	n.Uid = g.Uid
	n.Gid = g.Gid
	n.Children = g.Children
	return nil
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
	return info.node.Mtime()
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
		atime: now,
		mtime: now,
		ctime: now,
		Mode:  mode,
	}
}

func copyInode(source *iNode) *iNode {
	target := &iNode{
		Ino:      source.Ino,
		Mode:     source.Mode,
		Nlink:    source.Nlink,
		Size:     source.Size,
		Uid:      source.Uid,
		Gid:      source.Gid,
		Children: make(entries, len(source.Children)),
	}
	target.SetCtime(source.Ctime())
	target.SetAtime(source.Atime())
	target.SetMtime(source.Mtime())

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
		return 0, syscall.ENOTDIR
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

func (n *iNode) IsDir() bool {
	return os.ModeDir&n.Mode != 0
}

func (n *iNode) accessed() {
	n.SetAtime(time.Now())
}

func (n *iNode) modified() {
	now := time.Now()
	n.SetAtime(now)
	n.SetMtime(now)
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
