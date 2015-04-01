package main

import (
	"io"
	"log"
	"net/url"
	"os"

	"github.com/minio-io/cli"
	"github.com/minio-io/donut/client"
)

func upload(urlArg1, urlArg2 *url.URL) {
	st, stErr := os.Stat(urlArg1.Path)
	if os.IsNotExist(stErr) {
		log.Fatalln(stErr)
	}
	if st.IsDir() {
		log.Fatalln("is a directory")
	}
	reader, err := os.OpenFile(urlArg1.Path, 2, os.ModeAppend)
	defer reader.Close()
	if err != nil {
		log.Fatalln(err)
	}
	if urlArg2.Scheme == "donut" {
		donutConfigData, err := loadDonutConfig()
		if err != nil {
			log.Fatalln(err.Error())
		}
		if _, ok := donutConfigData.Donuts[urlArg2.Host]; !ok {
			log.Fatalf("requested donut: <%s> does not exist\n", urlArg2.Host)
		}
		nodes := make(map[string][]string)
		for k, v := range donutConfigData.Donuts[urlArg2.Host].Node {
			nodes[k] = v.ActiveDisks
		}
		d, err := client.GetNewClient(urlArg2.Host, nodes)
		if err != nil {
			log.Fatalln(err)
		}
		bucketName, objectName, err := url2Object(urlArg2.String())
		if err != nil {
			log.Fatalln(err)
		}
		if err := d.Put(bucketName, objectName, st.Size(), reader); err != nil {
			log.Fatalln(err)
		}
	}
}

func download(urlArg1, urlArg2 *url.URL) {
	writer, err := os.Create(urlArg2.Path)
	defer writer.Close()
	if err != nil {
		log.Fatalln(err)
	}
	if urlArg1.Scheme == "donut" {
		donutConfigData, err := loadDonutConfig()
		if err != nil {
			log.Fatalln(err)
		}
		if _, ok := donutConfigData.Donuts[urlArg1.Host]; !ok {
			log.Fatalf("requested donut: <%s> does not exist\n", urlArg1.Host)
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
		reader, size, err := d.Get(bucketName, objectName)
		if err != nil {
			log.Fatalln(err)
		}
		_, err = io.CopyN(writer, reader, size)
		if err != nil {
			log.Fatalln(err)
		}
		reader.Close()
	}
}

func doDonutCPCmd(c *cli.Context) {
	if !c.Args().Present() {
		log.Fatalln("no args?")
	}
	switch len(c.Args()) {
	case 2:
		urlArg1, errArg1 := url.Parse(c.Args().Get(0))
		if errArg1 != nil {
			log.Fatalln(errArg1)
		}
		urlArg2, errArg2 := url.Parse(c.Args().Get(1))
		if errArg2 != nil {
			log.Fatalln(errArg2)
		}
		switch true {
		case urlArg1.Scheme != "" && urlArg2.Scheme == "":
			download(urlArg1, urlArg2)
		case urlArg1.Scheme == "" && urlArg2.Scheme != "":
			upload(urlArg1, urlArg2)
		}
	}
}
