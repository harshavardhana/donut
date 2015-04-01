/*
 * Minimalist Object Storage, (C) 2014,2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"log"
	"time"

	"net/url"

	"github.com/cheggaaa/pb"
	"github.com/minio-io/cli"
	"github.com/minio-io/donut/client"
)

const (
	printDate = "2006-01-02 15:04:05 MST"
)

// printBuckets lists buckets and its meta-dat
func printBuckets(v []*client.Bucket) {
	for _, b := range v {
		fmt.Printf("%23s %13s %s\n", b.CreationDate.Time.Local().Format(printDate), "", b.Name)
	}
}

// printObjects prints a meta-data of a list of objects
func printObjects(v []*client.Item) {
	if len(v) > 0 {
		// Items are already sorted
		for _, b := range v {
			printObject(b.LastModified.Time, b.Size, b.Key)
		}
	}
}

// printObject prints object meta-data
func printObject(date time.Time, v int64, key string) {
	fmt.Printf("%23s %13s %s\n", date.Local().Format(printDate), pb.FormatBytes(v), key)
}

// doDonutListCmd - list buckets and objects
func doDonutListCmd(c *cli.Context) {
	if !c.Args().Present() {
		log.Fatalln("no args?")
	}
	urlArg1, err := url.Parse(c.Args().First())
	if err != nil {
		log.Fatalln(err)
	}
	donutConfigData, err := loadDonutConfig()
	if err != nil {
		log.Fatalln(err)
	}
	if _, ok := donutConfigData.Donuts[urlArg1.Host]; !ok {
		msg := fmt.Sprintf("requested donut: <%s> does not exist", urlArg1.Host)
		log.Fatalln(msg)
	}
	nodes := make(map[string][]string)
	for k, v := range donutConfigData.Donuts[urlArg1.Host].Node {
		nodes[k] = v.ActiveDisks
	}
	d, err := client.GetNewClient(urlArg1.Host, nodes)
	if err != nil {
		log.Fatalln(err)
	}
	bucketName, objectName, err := url2Object(urlArg1.String())
	if err != nil {
		log.Fatalln(err)
	}

	switch true {
	case bucketName == "":
		buckets, err := d.ListBuckets()
		if err != nil {
			log.Fatalln(err)
		}
		printBuckets(buckets)
	case objectName == "": // List objects in a bucket
		items, _, err := d.ListObjects(bucketName, "", "", "", client.Maxkeys)
		if err != nil {
			log.Fatalln(err)
		}
		printObjects(items)
	}
}
