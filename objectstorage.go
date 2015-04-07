package donut

import (
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/minio-io/iodine"
)

func (d donut) MakeBucket(bucket string) error {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return errors.New("invalid argument")
	}
	return d.makeBucket(bucket)
}

func (d donut) GetBucketMetadata(bucket string) (map[string]string, error) {
	return nil, errors.New("Not implemented")
}

func (d donut) SetBucketMetadata(bucket string, metadata map[string]string) error {
	return errors.New("Not implemented")
}

func (d donut) ListBuckets() (results []string, err error) {
	err = d.getAllBuckets()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	for name := range d.buckets {
		results = append(results, name)
	}
	sort.Strings(results)
	return results, nil
}

func (d donut) ListObjects(bucket, prefix, marker, delimiter string, maxkeys int) ([]string, bool, error) {
	// Marker and Delimiter are not yet handled please handle it
	errParams := map[string]string{
		"bucket":    bucket,
		"prefix":    prefix,
		"marker":    marker,
		"delimiter": delimiter,
		"maxkeys":   strconv.Itoa(maxkeys),
	}
	err := d.getAllBuckets()
	if err != nil {
		return nil, false, iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, false, iodine.New(errors.New("bucket does not exist"), errParams)
	}
	objectList, err := d.buckets[bucket].ListObjects()
	if err != nil {
		return nil, false, iodine.New(err, errParams)
	}
	var results []string
	var isTruncated bool
	for objectName := range objectList {
		if len(results) >= maxkeys {
			isTruncated = true
			break
		}
		if strings.HasPrefix(objectName, prefix) {
			results = append(results, objectName)
		}
	}
	sort.Strings(results)
	return results, isTruncated, nil
}

func (d donut) PutObject(bucket, object string, reader io.ReadCloser, metadata map[string]string) error {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return iodine.New(errors.New("invalid argument"), errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return iodine.New(errors.New("invalid argument"), errParams)
	}
	err := d.getAllBuckets()
	if err != nil {
		return iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return iodine.New(errors.New("bucket does not exist"), nil)
	}
	err = d.buckets[bucket].PutObject(object, reader, metadata)
	if err != nil {
		return iodine.New(err, errParams)
	}
	return nil
}

func (d donut) GetObject(bucket, object string) (reader io.ReadCloser, size int64, err error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return nil, 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return nil, 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	err = d.getAllBuckets()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, 0, iodine.New(errors.New("bucket does not exist"), errParams)
	}
	return d.buckets[bucket].GetObject(object)
}

func (d donut) GetObjectMetadata(bucket, object string) (map[string]string, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	err := d.getAllBuckets()
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, iodine.New(errors.New("bucket does not exist"), errParams)
	}
	objectList, err := d.buckets[bucket].ListObjects()
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	objectStruct, ok := objectList[object]
	if !ok {
		return nil, iodine.New(errors.New("object does not exist"), errParams)
	}
	return objectStruct.GetDonutObjectMetadata()
}
