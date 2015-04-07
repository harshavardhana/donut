package donut

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

func (d donut) makeBucket(bucketName string) error {
	err := d.getAllBuckets()
	if err != nil {
		return err
	}
	if _, ok := d.buckets[bucketName]; ok {
		return errors.New("bucket exists")
	}
	bucket, err := NewBucket(bucketName, d.name, d.nodes)
	if err != nil {
		return err
	}
	nodeNumber := 0
	d.buckets[bucketName] = bucket
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return err
		}
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", bucketName, nodeNumber, disk.GetOrder())
			err := disk.MakeDir(path.Join(d.name, bucketSlice))
			if err != nil {
				return err
			}
		}
		nodeNumber = nodeNumber + 1
	}
	return nil
}

func (d donut) getAllBuckets() error {
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return err
		}
		for _, disk := range disks {
			dirs, err := disk.ListDir(d.name)
			if err != nil {
				return err
			}
			for _, dir := range dirs {
				splitDir := strings.Split(dir.Name(), "$")
				if len(splitDir) < 3 {
					return errors.New("corrupted backend")
				}
				bucketName := splitDir[0]
				// we dont need this NewBucket once we cache these
				bucket, err := NewBucket(bucketName, d.name, d.nodes)
				if err != nil {
					return err
				}
				d.buckets[bucketName] = bucket
			}
		}
	}
	return nil
}
