package donut

import (
	//	"bytes"
	//	"io"
	"io/ioutil"
	"os"
	"testing"
	//	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func setupNodeDiskMap(c *C) map[string][]string {
	var disks []string
	for i := 0; i < 16; i++ {
		root, err := ioutil.TempDir(os.TempDir(), "donut-")
		c.Assert(err, IsNil)
		disks = append(disks, root)
	}
	nodeDiskMap := make(map[string][]string)
	nodeDiskMap["localhost"] = disks
	return nodeDiskMap
}

func removeDisks(c *C, disks []string) {
	for _, disk := range disks {
		err := os.RemoveAll(disk)
		c.Assert(err, IsNil)
	}
}

func (s *MySuite) TestEmptyBucket(c *C) {
	nodeDiskMap := setupNodeDiskMap(c)
	donut, err := NewDonut("testemptydonut", nodeDiskMap)
	defer removeDisks(c, nodeDiskMap["localhost"])
	c.Assert(err, IsNil)

	// check buckets are empty
	buckets, err := donut.ListBuckets()
	c.Assert(err, IsNil)
	c.Assert(len(buckets), Equals, 0)
}

func (s *MySuite) TestBucketWithoutNameFails(c *C) {
	nodeDiskMap := setupNodeDiskMap(c)
	donut, err := NewDonut("testemptydonut", nodeDiskMap)
	defer removeDisks(c, nodeDiskMap["localhost"])
	c.Assert(err, IsNil)

	// fail to create new bucket without a name
	err = donut.MakeBucket("")
	c.Assert(err, Not(IsNil))

	err = donut.MakeBucket(" ")
	c.Assert(err, Not(IsNil))
}

func (s *MySuite) TestCreateBucketAndList(c *C) {
	nodeDiskMap := setupNodeDiskMap(c)
	donut, err := NewDonut("testemptydonut", nodeDiskMap)
	defer removeDisks(c, nodeDiskMap["localhost"])
	c.Assert(err, IsNil)

	// make bucket
	err = donut.MakeBucket("foo")
	c.Assert(err, IsNil)

	// check bucket exists
	buckets, err := donut.ListBuckets()
	c.Assert(err, IsNil)
	_, ok := buckets["foo"]
	c.Assert(ok, Equals, true)
}

func (s *MySuite) TestCreateBucketWithSameNameFails(c *C) {
	nodeDiskMap := setupNodeDiskMap(c)
	donut, err := NewDonut("testemptydonut", nodeDiskMap)
	defer removeDisks(c, nodeDiskMap["localhost"])
	c.Assert(err, IsNil)

	// make bucket
	err = donut.MakeBucket("foo")
	c.Assert(err, IsNil)

	// make bucket fail
	err = donut.MakeBucket("foo")
	c.Assert(err, Not(IsNil))
}

func (s *MySuite) TestCreateMultipleBucketsAndList(c *C) {
	nodeDiskMap := setupNodeDiskMap(c)
	donut, err := NewDonut("testemptydonut", nodeDiskMap)
	defer removeDisks(c, nodeDiskMap["localhost"])
	c.Assert(err, IsNil)

	err = donut.MakeBucket("foo")
	c.Assert(err, IsNil)

	err = donut.MakeBucket("bar")
	c.Assert(err, IsNil)

	buckets, err := donut.ListBuckets()
	c.Assert(err, IsNil)

	createdBuckets := []string{"bar", "foo"}
	for _, bucketName := range createdBuckets {
		_, ok := buckets[bucketName]
		c.Assert(ok, Equals, true)
	}

	err = donut.MakeBucket("foobar")
	c.Assert(err, IsNil)
	createdBuckets = append(createdBuckets, "foobar")

	buckets, err = donut.ListBuckets()
	c.Assert(err, IsNil)
	for _, bucketName := range createdBuckets {
		_, ok := buckets[bucketName]
		c.Assert(ok, Equals, true)
	}
}

/*
func (s *MySuite) TestNewObjectFailsWithoutBucket(c *C) {
	nodeDiskMap := setupNodeDiskMap(c)
	donut, err := NewDonut("testemptydonut", nodeDiskMap)
	defer removeDisks(c, nodeDiskMap["localhost"])
	c.Assert(err, IsNil)

	reader, writer := io.Pipe()
	err := donut.GetObject("foo", "obj")
	c.Assert(err, Not(IsNil))
	c.Assert(writer, IsNil)
}

func (s *MySuite) TestNewObjectFailsWithEmptyName(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut(root)
	c.Assert(err, IsNil)

	writer, err := donut.GetObjectWriter("foo", "")
	c.Assert(err, Not(IsNil))
	c.Assert(writer, IsNil)

	writer, err = donut.GetObjectWriter("foo", " ")
	c.Assert(err, Not(IsNil))
	c.Assert(writer, IsNil)
}

func (s *MySuite) TestNewObjectCanBeWritten(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut(root)
	c.Assert(err, IsNil)

	err = donut.CreateBucket("foo")
	c.Assert(err, IsNil)

	writer, err := donut.GetObjectWriter("foo", "obj")
	c.Assert(err, IsNil)

	data := "Hello World"
	length, err := writer.Write([]byte(data))
	c.Assert(length, Equals, len(data))

	expectedMetadata := map[string]string{
		"foo":     "bar",
		"created": "one",
		"hello":   "world",
	}

	err = writer.SetMetadata(expectedMetadata)
	c.Assert(err, IsNil)

	err = writer.Close()
	c.Assert(err, IsNil)

	actualWriterMetadata, err := writer.GetMetadata()
	c.Assert(err, IsNil)
	c.Assert(actualWriterMetadata, DeepEquals, expectedMetadata)

	c.Assert(err, IsNil)

	reader, err := donut.GetObjectReader("foo", "obj")
	c.Assert(err, IsNil)

	var actualData bytes.Buffer
	_, err = io.Copy(&actualData, reader)
	c.Assert(err, IsNil)
	c.Assert(actualData.Bytes(), DeepEquals, []byte(data))

	actualMetadata, err := donut.GetObjectMetadata("foo", "obj")
	c.Assert(err, IsNil)
	expectedMetadata["sys.md5"] = "b10a8db164e0754105b7a99be72e3fe5"
	expectedMetadata["sys.size"] = "11"
	_, err = time.Parse(time.RFC3339Nano, actualMetadata["sys.created"])
	c.Assert(err, IsNil)
	expectedMetadata["sys.created"] = actualMetadata["sys.created"]
	c.Assert(actualMetadata, DeepEquals, expectedMetadata)
}

func (s *MySuite) TestMultipleNewObjects(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut(root)
	c.Assert(err, IsNil)

	c.Assert(donut.CreateBucket("foo"), IsNil)
	writer, err := donut.GetObjectWriter("foo", "obj1")
	c.Assert(err, IsNil)
	writer.Write([]byte("one"))
	writer.Close()

	writer, err = donut.GetObjectWriter("foo", "obj2")
	c.Assert(err, IsNil)
	writer.Write([]byte("two"))
	writer.Close()

	//	c.Skip("not complete")

	reader, err := donut.GetObjectReader("foo", "obj1")
	c.Assert(err, IsNil)
	var readerBuffer1 bytes.Buffer
	_, err = io.Copy(&readerBuffer1, reader)
	c.Assert(err, IsNil)
	//	c.Skip("Not Implemented")
	c.Assert(readerBuffer1.Bytes(), DeepEquals, []byte("one"))

	reader, err = donut.GetObjectReader("foo", "obj2")
	c.Assert(err, IsNil)
	var readerBuffer2 bytes.Buffer
	_, err = io.Copy(&readerBuffer2, reader)
	c.Assert(err, IsNil)
	c.Assert(readerBuffer2.Bytes(), DeepEquals, []byte("two"))

	// test list objects
	listObjects, err := donut.ListObjects("foo")
	c.Assert(err, IsNil)
	c.Assert(listObjects, DeepEquals, []string{"obj1", "obj2"})
}

func (s *MySuite) TestSysPrefixShouldFail(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut(root)
	c.Assert(err, IsNil)

	c.Assert(donut.CreateBucket("foo"), IsNil)
	writer, err := donut.GetObjectWriter("foo", "obj1")
	c.Assert(err, IsNil)
	writer.Write([]byte("one"))
	metadata := make(map[string]string)
	metadata["foo"] = "bar"
	metadata["sys.hello"] = "world"
	err = writer.SetMetadata(metadata)
	c.Assert(err, Not(IsNil))
	writer.Close()
}
*/
