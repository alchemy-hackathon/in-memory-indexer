package main

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

type Block struct {
	transactions     []Transaction
	alch_blocknumber int
}

type Transaction struct {
	logs []Log
	hash string
}

func (a Transaction) Value() (driver.Value, error) {
	return json.Marshal(a)
}

// Make the Attrs struct implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (a *Transaction) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &a)
}

type Log struct {
	logIndex string
	address  string
	topics   []string
	data     string
}

func processBlocks(wg *sync.WaitGroup, fileName string, processor BlockProcessor) (int, error) {
	defer wg.Done()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, err
	}
	defer db.Close()

	result, err := db.Query(fmt.Sprintf(`
		SELECT *
		FROM '%s'
	`, fileName))
	defer result.Close()

	if err != nil {
		return 0, err
	}
	log.Printf("Finished getting blocks from file %s.", fileName)

	var numBlocks = 0

	for result.Next() {
		block := new(Block)
		transactionInterfaces := make([]interface{}, 0)
		err := result.Scan(&transactionInterfaces, &block.alch_blocknumber)
		if err != nil {
			return 0, err
		}

		transactions := deserializeTransactions(transactionInterfaces)
		block.transactions = transactions

		err = processor.ProcessBlock(block)
		if err != nil {
			return 0, err
		}
		numBlocks += 1
	}
	log.Printf("Finished indexing blocks in file %s. Now flushing to DB.", fileName)

	err = Retry(func() error {
		err = processor.FlushToDb()
		if err != nil {
			return err
		}

		return nil
	}, 5, 2*time.Second)

	return numBlocks, err
}

func deserializeTransactions(transactions interface{}) []Transaction {
	transactionsArray := transactions.([]interface{})
	var parsedTransactions = make([]Transaction, 0)

	for _, transaction := range transactionsArray {
		var parsedLogs = make([]Log, 0)

		transactionLogs := transaction.(map[string]interface{})["logs"].([]interface{})
		for _, transactionLog := range transactionLogs {
			logIndex := transactionLog.(map[string]interface{})["logindex"].(string)
			address := transactionLog.(map[string]interface{})["address"].(string)
			topicsInterface := transactionLog.(map[string]interface{})["topics"].([]interface{})
			data := transactionLog.(map[string]interface{})["data"].(string)
			var topics = make([]string, 0)
			for _, topicInterface := range topicsInterface {
				topics = append(topics, topicInterface.(string))
			}
			parsedLogs = append(parsedLogs, Log{
				address:  address,
				logIndex: logIndex,
				topics:   topics,
				data:     data,
			})
		}

		parsedTransactions = append(parsedTransactions, Transaction{
			logs: parsedLogs,
			hash: transaction.(map[string]interface{})["hash"].(string),
		})
	}

	return parsedTransactions
}

func main() {
	dbConnStr := os.Getenv(("DB_CONN_STR"))
	if dbConnStr == "" {
		log.Fatal("DB_CONN_STR environment variable not set.")
	}
	if os.Args[1] == "" {
		log.Fatal("Max concurrency not set. Please provide as first argument.")
	}

	go func() {
		http.ListenAndServe("localhost:8080", nil)
	}()

	start := time.Now()

	blocksPath := "./blocks/"
	// Get all files in currently directory
	files, err := os.ReadDir(blocksPath)
	if err != nil {
		log.Fatal(err)
	}

	maxConcurrency, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Starting to process with file concurrency %d.", maxConcurrency)

	guard := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < len(files); i++ {
		guard <- struct{}{} // Will block if guard channel is already filled.
		fileName := files[i].Name()

		wg.Add(1)
		go func(n int) {
			log.Printf("[%d / %d] Processing blocks in file %s.", n+1, len(files), fileName)

			chunkProcessingStart := time.Now()

			processor, err := NewNft1155OwnershipBlockProcessor(dbConnStr)
			if err != nil {
				log.Fatal(err)
			}

			numBlocks, err := processBlocks(&wg, blocksPath+fileName, processor)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("[%d / %d] Finished processing %d blocks in file %s. Took %v.", n+1, len(files), numBlocks, fileName, time.Since(chunkProcessingStart))

			<-guard
		}(i)
	}

	wg.Wait()
	log.Printf("Job finished. Total time: %v.", time.Since(start))
	// processor.DebugPrintResults()
}
