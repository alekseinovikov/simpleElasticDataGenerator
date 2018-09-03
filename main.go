package main

import (
	"context"
	"github.com/olivere/elastic"
	"github.com/satori/go.uuid"
	"log"
	"math/rand"
	"time"
)

const INDEX_NAME = "notes"
const TYPE_NAME = "note"
const NUMBER_OF_CLIENTS = 10
const NUMBER_OF_PORTION = 1000
const NUMBER = 1000000000
const MAX_STRING_LENGTH = 1000

type Note struct {
	Id      int    `json:"id"`
	Content string `json:"content"`
}

var (
	clientChannel     chan *elastic.Client
	freeClientChannel chan interface{}
)

func init() {
	rand.Seed(time.Now().UnixNano())
	clientChannel = make(chan *elastic.Client, NUMBER_OF_CLIENTS)
	freeClientChannel = make(chan interface{})
}

func clearIndex(client *elastic.Client) {
	query := elastic.NewExistsQuery("content")
	client.DeleteByQuery(INDEX_NAME).Query(query).Do(context.Background())
}

func main() {
	//clearIndex(createClient())
	initClientGenerator()
	startGeneration(NUMBER)
}

func startGeneration(count int) {
	counter := 0
	resultChannel := make(chan int)
	for count > 0 {
		portion := NUMBER_OF_PORTION
		if portion > count {
			portion = count
		}

		count -= portion

		go generate(portion, resultChannel)
		counter++
	}

	processedCount := 0
	for i := 0; i < counter; i++ {
		processedCount += <-resultChannel

		log.Println(processedCount)
	}

	close(resultChannel)
}

func generate(count int, resultChannel chan int) {
	client := getClient()
	defer freeClient()

	bulkService := createBulkService(client)

	for i := 0; i < count; i++ {
		insertToBulkIndex(bulkService, i, uuid.NewV4().String())
	}

	flushBulkService(bulkService)

	resultChannel <- count
}

func createBulkService(client *elastic.Client) *elastic.BulkService {
	return client.Bulk().Index(INDEX_NAME).Type(TYPE_NAME)
}

func insertToBulkIndex(bulkService *elastic.BulkService, id int, content string) {
	note := createRandomNote()
	bulkService.Add(elastic.NewBulkIndexRequest().Doc(note))
}

func createRandomNote() Note {
	return Note{Id: rand.Int(), Content: randStringRunes(rand.Intn(MAX_STRING_LENGTH))}
}

func flushBulkService(service *elastic.BulkService) {
	_, err := service.
		Do(context.TODO())

	if nil != err {
		log.Fatal(err)
	}
}

func initClientGenerator() {
	for i := 0; i < NUMBER_OF_CLIENTS; i++ {
		clientChannel <- createClient()
	}

	go func() {
		for {
			select {
			case <-freeClientChannel:
				clientChannel <- createClient()
			}
		}
	}()
}

func createClient() *elastic.Client {
	client, err := elastic.NewClient()
	if nil != err {
		log.Fatal(err)
	}

	return client
}

func getClient() *elastic.Client {
	return <-clientChannel
}

func freeClient() {
	freeClientChannel <- true
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
