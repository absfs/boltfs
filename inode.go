package boltfs

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/xtgo/set"
)

type inode struct {
	Size   int64
	Atime  time.Time
	Mtime  time.Time
	Mode   os.FileMode
	Digest c4.Digest
}

var emptydigest c4.Digest = c4.Identify(bytes.NewReader([]byte{})).Digest()

func newDirNode(mode os.FileMode) *inode {
	node := &inode{
		Mode: mode | os.ModeDir,
	}
	node.SetDigest(emptydigest)
	return node
}

func newNode(mode os.FileMode) *inode {
	node := &inode{
		Mode: mode,
	}
	node.SetDigest(emptydigest)

	return node
}

func (n *inode) SetDigest(digest c4.Digest) bool {
	if bytes.Compare(n.Digest, digest) == 0 {
		return false
	}
	n.Digest = c4.NewDigest(digest)

	n.SetAccess(time.Time{})
	n.SetModified(time.Time{})
	return true
}

func (n *inode) SetAccess(t time.Time) {
	if t.IsZero() {
		n.Atime = time.Now().UTC()
		return
	}
	n.Atime = t.UTC()
}

func (n *inode) SetModified(t time.Time) {
	if t.IsZero() {
		n.Mtime = time.Now().UTC()
		return
	}
	n.Mtime = t.UTC()
}

func (n *inode) String() string {
	return fmt.Sprintf("%s % 6d\t%s\tModTime %s, AccessTime %s", n.Digest.ID().String()[:9], n.Size, n.Mode, n.Mtime.Local().Format(time.UnixDate), n.Mtime.Local().Format(time.UnixDate))
}

func (n *inode) Bytes() []byte {
	data, err := n.MarshalBinary()
	if err != nil {
		return nil
	}
	return data
}

func (n *inode) MarshalBinary() ([]byte, error) {
	data := make([]byte, 106)

	binary.LittleEndian.PutUint64(data[:8], uint64(n.Size))

	tdata, err := n.Atime.MarshalBinary()
	if err != nil {
		return nil, err
	}
	copy(data[8:23], tdata)

	tdata, err = n.Mtime.MarshalBinary()
	if err != nil {
		return nil, err
	}
	copy(data[23:38], tdata)

	binary.LittleEndian.PutUint32(data[38:42], uint32(n.Mode))
	copy(data[42:106], n.Digest)

	return data, nil
}

func (n *inode) UnmarshalBinary(data []byte) error {
	if len(data) != 106 {
		return io.EOF
	}
	n.Size = int64(binary.LittleEndian.Uint64(data))
	data = data[8:]
	n.Atime.UnmarshalBinary(data[:15])
	data = data[15:]
	n.Mtime.UnmarshalBinary(data[:15])
	data = data[15:]
	n.Mode = os.FileMode(binary.LittleEndian.Uint32(data[:4]))
	data = data[4:]
	n.Digest = c4.NewDigest(data)
	return nil
}

type Dir []Entry

func (d Dir) Len() int           { return len(d) }
func (d Dir) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d Dir) Less(i, j int) bool { return d[i].Name < d[j].Name }

func (d Dir) String() string {
	var list []string
	for _, entry := range d {
		list = append(list, "{"+entry.String()+"}")
	}
	return "Dir{" + strings.Join(list, ",") + "}"
}

func (d Dir) Find(name string) (id uint64, ok bool) {
	if !sort.IsSorted(d) {
		d.Sort()
	}

	x := sort.Search(d.Len(), func(i int) bool {
		return name <= d[i].Name
	})

	if x == d.Len() || d[x].Name != name {
		return 0, false
	}
	return d[x].Id, true
}

func (d *Dir) Sort() {
	sort.Sort(*d)
	n := set.Uniq(*d)
	(*d) = (*d)[:n]
}

func (dir Dir) Bytes() []byte {
	w := new(bytes.Buffer)
	err := gob.NewEncoder(w).Encode(dir)

	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

type Entry struct {
	depth int

	Id   uint64
	Name string
}

func (e *Entry) SetDepth(depth int) {
	e.depth = depth
}

func (e *Entry) Depth() int {
	return e.depth
}

func (e *Entry) String() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%q: %d", e.Name, e.Id)
}
