package main

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

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
	log.Println("Starting...")

	processor := NewNft1155OwnershipBlockProcessor()

	blocksPath := "./blocks/"
	// Get all files in currently directory
	files, err := os.ReadDir(blocksPath)
	if err != nil {
		log.Fatal(err)
	}

	maxConcurrency := 10
	guard := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < len(files); i++ {
		guard <- struct{}{} // Will block if guard channel is already filled.
		fileName := files[i].Name()

		wg.Add(1)
		go func(n int) {
			log.Printf("Processing blocks in file %s.", fileName)
			numBlocks, err := processBlocks(&wg, blocksPath+fileName, &processor)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Finished processing %d blocks in file %s.", numBlocks, fileName)
			<-guard
		}(i)
	}

	wg.Wait()
	// processor.DebugPrintResults()
}
