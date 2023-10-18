package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const Erc1155TransferAbi = `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256[]","name":"ids","type":"uint256[]"},{"indexed":false,"internalType":"uint256[]","name":"values","type":"uint256[]"}],"name":"TransferBatch","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"id","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"TransferSingle","type":"event"}]`
const TransferSingleTopic0 = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
const TransferBatchTopic0 = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

// const Erc20TransferAbi = `[{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]`
// const TransferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

type BlockProcessor interface {
	ProcessBlock(block *Block) error
	FlushOwnershipToDb() error
	DebugPrintResults()
}

type TokenOwner struct {
	owner           string
	contractAddress string
	tokenID         string
}

type Nft1155OwnershipBlockProcessor struct {
	// Map of TokenOwner -> number of tokens owned.
	tokenOwners map[TokenOwner]*big.Int
	mutex       sync.RWMutex
	db          *pgxpool.Pool
}

func NewNft1155OwnershipBlockProcessor(dbConnStr string) (*Nft1155OwnershipBlockProcessor, error) {
	db, err := pgxpool.Connect(context.Background(), dbConnStr)

	if err != nil {
		return nil, err
	}
	return &Nft1155OwnershipBlockProcessor{
		tokenOwners: make(map[TokenOwner]*big.Int),
		db:          db,
	}, nil
}

func (p *Nft1155OwnershipBlockProcessor) ProcessBlock(block *Block) error {
	// iterate through logs in transactions in the block and skip logs that don't have topic0 == TransferSingle
	for _, transaction := range block.transactions {
		for _, eventLog := range transaction.logs {
			// Logs without topics are definitely not transfer events.
			if len(eventLog.topics) == 0 {
				continue
			}

			switch eventLog.topics[0] {
			// case TransferTopic0:
			// 	value, err := decodeTransferEventData(eventLog.data)
			// 	if err != nil {
			// 		log.Printf("transaction hash=%s, logIndex=%s, logData=%s, topic0=%s, topic1=%s, topic2=%s", transaction.hash, eventLog.logIndex, eventLog.data, eventLog.topics[0], eventLog.topics[1], eventLog.topics[2])
			// 		return err
			// 	}
			// 	log.Printf("Transfer event: value=%d, transactionHash=%s, logIndex=%s", value, transaction.hash, eventLog.logIndex)
			case TransferSingleTopic0:
				from := eventLog.topics[1]
				to := eventLog.topics[2]
				id, value, err := decodeTransferSingleEventData(eventLog.data)
				if err != nil {
					return err
				}

				// log.Printf("TransferSingle event: from=%s, to=%s, id=%s, value=%s", from, to, id, value)
				owner := TokenOwner{
					owner:           to,
					contractAddress: eventLog.address,
					tokenID:         id.String(),
				}
				p.upsertOwnership(owner, value)
				formerOwner := TokenOwner{
					owner:           from,
					contractAddress: eventLog.address,
					tokenID:         id.String(),
				}
				p.upsertOwnership(formerOwner, new(big.Int).Set(value).Neg(value))
			case TransferBatchTopic0:
				from := eventLog.topics[1]
				to := eventLog.topics[2]
				ids, values, err := decodeTransferBatchEventData(eventLog.data)
				if err != nil {
					return err
				}

				for i, id := range ids {
					owner := TokenOwner{
						owner:           to,
						contractAddress: eventLog.address,
						tokenID:         id.String(),
					}
					p.upsertOwnership(owner, values[i])
					formerOwner := TokenOwner{
						owner:           from,
						contractAddress: eventLog.address,
						tokenID:         id.String(),
					}
					p.upsertOwnership(formerOwner, new(big.Int).Set(values[i]).Neg(values[i]))
				}
			default:
				continue
			}
		}
	}

	return nil
}

func decodeTransferSingleEventData(hexData string) (id *big.Int, value *big.Int, err error) {
	data, err := hex.DecodeString(hexData[2:])
	if err != nil {
		log.Printf("Error decoding hex data. hexData=%s, lobbed=%s", hexData, hexData[2:])
		return nil, nil, err
	}

	abi, err := abi.JSON(strings.NewReader(Erc1155TransferAbi))
	if err != nil {
		return nil, nil, err
	}

	event := map[string]interface{}{}
	if err := abi.UnpackIntoMap(event, "TransferSingle", data); err != nil {
		log.Printf("Error unpacking data into map. data=%s, hexData=%s, err=%s", data, hexData, err)
		return nil, nil, err
	}

	return event["id"].(*big.Int), event["value"].(*big.Int), nil
}

func decodeTransferBatchEventData(hexData string) (ids []*big.Int, values []*big.Int, err error) {
	data, err := hex.DecodeString(hexData[2:])
	if err != nil {
		log.Printf("Error decoding hex data. hexData=%s, lobbed=%s", hexData, hexData[2:])
		return nil, nil, err
	}

	abi, err := abi.JSON(strings.NewReader(Erc1155TransferAbi))
	if err != nil {
		return nil, nil, err
	}

	event := map[string]interface{}{}
	if err := abi.UnpackIntoMap(event, "TransferBatch", data); err != nil {
		log.Printf("Error unpacking data into map. data=%s, hexData=%s, err=%s", data, hexData, err)
		return nil, nil, err
	}

	return event["ids"].([]*big.Int), event["values"].([]*big.Int), nil
}

// func decodeTransferEventData(hexData string) (value *big.Int, err error) {
// 	data, err := hex.DecodeString(hexData[2:])
// 	// Needs special handling of data = 0 because the ABI decoder can't handle it.
// 	if hexData[2:] == "" {
// 		return big.NewInt(0), nil
// 	}

// 	if err != nil {
// 		log.Printf("Error decoding hex data. hexData=%s, lobbed=%s", hexData, hexData[2:])
// 		return nil, err
// 	}

// 	abi, err := abi.JSON(strings.NewReader(Erc20TransferAbi))
// 	if err != nil {
// 		log.Printf("Error parsing abi. %s", err)
// 		return nil, err
// 	}

// 	event := map[string]interface{}{}
// 	if err := abi.UnpackIntoMap(event, "Transfer", data); err != nil {
// 		log.Printf("Error unpacking data into map. data=%s, hexData=%s, err=%s", data, hexData, err)
// 		return nil, err
// 	}

// 	valueInt := event["value"].(*big.Int)
// 	return valueInt, nil
// }

func (p *Nft1155OwnershipBlockProcessor) upsertOwnership(tokenOwner TokenOwner, numTokens *big.Int) {
	p.mutex.Lock()
	_, ok := p.tokenOwners[tokenOwner]
	if ok {
		p.tokenOwners[tokenOwner].Add(p.tokenOwners[tokenOwner], numTokens)
	} else {
		p.tokenOwners[tokenOwner] = numTokens
	}
	p.mutex.Unlock()
}

// Yeah these queries are prone to SQL injection but it's easier to write.
func (p *Nft1155OwnershipBlockProcessor) FlushOwnershipToDb() error {
	rows := [][]any{}
	columns := []string{"owner_address", "contract_address", "token_id", "count"}
	tableName := "token_owner"
	tempTableName := tableName + "_temp"

	p.mutex.Lock()
	for tokenOwner, numTokens := range p.tokenOwners {
		numTokensNumeric := new(pgtype.Numeric)
		err := numTokensNumeric.Set(numTokens.String())
		if err != nil {
			return fmt.Errorf("Error creating numTokensNumeric: %w", err)
		}
		rows = append(rows, []any{tokenOwner.owner, tokenOwner.contractAddress, tokenOwner.tokenID, numTokensNumeric})
	}
	p.tokenOwners = make(map[TokenOwner]*big.Int)
	p.mutex.Unlock()

	transaction, err := p.db.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("Error beginning transaction: %w", err)
	}

	defer transaction.Rollback(context.Background())

	_, err = transaction.Exec(context.Background(), fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (LIKE %s INCLUDING ALL) ON COMMIT DROP
	`, tempTableName, tableName))
	if err != nil {
		return fmt.Errorf("Error creating temporary table: %w", err)
	}

	_, err = transaction.CopyFrom(
		context.Background(),
		pgx.Identifier{tempTableName},
		columns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("Error copying rows: %w", err)
	}

	_, err = transaction.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (owner_address, contract_address, token_id, count)
		(
			SELECT owner_address, contract_address, token_id, count
			FROM %s
		)
		ON CONFLICT (owner_address, contract_address, token_id)
		DO UPDATE SET count = %s.count + EXCLUDED.count;	
	`, tableName, tempTableName, tableName))
	if err != nil {
		return fmt.Errorf("Error upserting rows: %w", err)
	}

	transaction.Commit(context.Background())
	// We should never use this connection pool again after this function is called.
	p.db.Close()
	return nil
}

func (p *Nft1155OwnershipBlockProcessor) GetOwnerCount() int {
	return len(p.tokenOwners)
}

func (p *Nft1155OwnershipBlockProcessor) DebugPrintResults() {
	p.mutex.RLock()
	for tokenOwner, numTokens := range p.tokenOwners {
		log.Printf("TokenOwner: %s, %s, %s, %s", tokenOwner.owner, tokenOwner.contractAddress, tokenOwner.tokenID, numTokens)
	}
	p.mutex.RUnlock()
}
