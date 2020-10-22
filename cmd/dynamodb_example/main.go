package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/genji1037/dynanodb-example/alg"
	"github.com/genji1037/dynanodb-example/progress"
	"github.com/genji1037/dynanodb-example/storage"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/cobra"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultPayload = "0x5b7b22636f6e766572736174696f6e223a2263663134393963612d636339612d343233322d393233352d623730376533616231313936222c2274696d65223a22323032302d31302d31335430373a35363a33332e3934395a222c2264617461223a7b22757365725f6e616d6573223a5b2262673530303434225d2c22757365725f696473223a5b2232663766666439302d383866652d346630362d623236632d353666633138623364366134225d2c226d656d73756d223a36353337397d2c22656964223a2234373633343933372d333530612d343262382d623236652d303236326430343137313365222c2266726f6d223a2232663766666439302d383866652d346630362d623236632d353666633138623364366134222c22636f6e7674797065223a352c2274797065223a22636f6e766572736174696f6e2e6d656d6265722d6c65617665227d5d"
)

func main() {
	var rootCmd = &cobra.Command{Use: "dynamodb_example"}

	// bgp sub command
	{
		var cnvID, nid, payload, input, output string
		var limit, concurrency int64
		var num int

		var cmdBGP = &cobra.Command{
			Use:   "bgp",
			Short: "bgp_notification",
		}

		var cmdCreate = &cobra.Command{
			Use:   "create",
			Short: "create table",
			Run: func(cmd *cobra.Command, args []string) {
				db := storage.NewDB()
				db.CreateBGPNotificationTable()
			},
		}

		var cmdDesc = &cobra.Command{
			Use:   "desc",
			Short: "describe table",
			Run: func(cmd *cobra.Command, args []string) {
				db := storage.NewDB()
				db.DescribeBGPNotificationTable()
			},
		}

		var cmdQuery = &cobra.Command{
			Use:   "query",
			Short: "query table",
			Run: func(cmd *cobra.Command, args []string) {
				if num > 1 { // query benchmark
					db := storage.NewDB()
					wg := sync.WaitGroup{}
					wg.Add(num)
					maxInflight := make(chan struct{}, concurrency)
					startAt := time.Now()
					for i := 0; i < num; i++ {
						maxInflight <- struct{}{}
						go func() {
							db.QueryBGPNotificationsByCnvID(cnvID, nid, limit)
							<-maxInflight
							wg.Done()
						}()
					}
					progress.P.TimeTotal = time.Now().Sub(startAt)
					progress.Report()
				} else { // simple query
					db := storage.NewDB()
					result := db.QueryBGPNotificationsByCnvID(cnvID, nid, limit)
					fmt.Println(result)
				}
			},
		}
		cmdQuery.Flags().StringVarP(&cnvID, "cnv_id", "v", "dcdcf72b-ee08-46b9-9567-4b950207bc07", "conversation id")
		cmdQuery.Flags().StringVarP(&nid, "nid", "s", "7aa30ce0-0eb8-11eb-86a9-a45e60ea5ac3", "since timestamp")
		cmdQuery.Flags().Int64VarP(&limit, "limit", "l", 500, "page size")
		cmdQuery.Flags().Int64VarP(&concurrency, "concurrency", "c", 16, "concurrency")
		cmdQuery.Flags().IntVarP(&num, "num", "n", 1, "times you wants to read")

		var cmdPut = &cobra.Command{
			Use:   "put",
			Short: "put item to table",
			Run: func(cmd *cobra.Command, args []string) {
				db := storage.NewDB()
				db.PutBGPNotification(storage.Notification{
					CnvID:   cnvID,
					Payload: payload,
				})
			},
		}
		cmdPut.Flags().StringVarP(&cnvID, "cnv_id", "c", "dcdcf72b-ee08-46b9-9567-4b950207bc07", "conversation id")
		cmdPut.Flags().StringVarP(&payload, "payload", "p", "0x12345", "message payload")

		var cmdImport = &cobra.Command{
			Use:   "import",
			Short: "import from file to table",
			Run: func(cmd *cobra.Command, args []string) {
				f, err := os.Open(input)
				if err != nil {
					log.Printf("open input file file: %v", err)
					return
				}
				defer f.Close()
				rd := bufio.NewReader(f)
				nCh := make(chan storage.Notification)
				go func() {
					for {
						line, _, err := rd.ReadLine()
						if err != nil {
							if err == io.EOF {
								break
							}
							log.Printf("read input file line failed: %v", err)
							return
						}
						tmp := strings.Split(string(line), ",")
						if len(tmp) != 3 {
							log.Printf("bad input file format %s", string(line))
						}
						nCh <- storage.Notification{
							CnvID:   tmp[0],
							NID:     tmp[1],
							Payload: tmp[2],
						}
					}
					close(nCh)
				}()
				db := storage.NewDB()
				maxInflight := make(chan struct{}, concurrency)
				wg := sync.WaitGroup{}
				startAt := time.Now()
				for n := range nCh {
					maxInflight <- struct{}{}
					wg.Add(1)
					go func(n storage.Notification) {
						db.PutBGPNotification(n)
						<-maxInflight
						wg.Done()
					}(n)
				}
				wg.Wait()
				progress.P.TimeTotal = time.Now().Sub(startAt)
				progress.Report()
			},
		}
		cmdImport.Flags().StringVarP(&input, "file", "f", "bgpnotification.csv", "input file path")
		cmdImport.Flags().Int64VarP(&concurrency, "concurrency", "c", 16, "concurrency")

		var cmdMock = &cobra.Command{
			Use:   "mock",
			Short: "generate mock data and import to table",
			Run: func(cmd *cobra.Command, args []string) {
				f, err := os.Create(output)
				if err != nil {
					log.Printf("create output file failed: %v", err)
					return
				}
				defer f.Close()

				// generate random cnvs
				cnvNum := int(alg.Sqrt(float64(num)))
				if cnvNum <= 0 {
					cnvNum = 1
				}
				cnvs := make([]string, cnvNum)
				for i := range cnvs {
					cnvs[i] = uuid.NewV4().String()
				}

				// generate records
				rand.Seed(time.Now().UnixNano())
				epoch := time.Now().UnixNano() / 1000000
				for i := 0; i < num; i++ {
					cnvID := cnvs[rand.Intn(len(cnvs))]
					nID := epoch + int64(i)
					payload := defaultPayload
					_, err := f.WriteString(fmt.Sprintf("%s,%d,%s\n", cnvID, nID, payload))
					if err != nil {
						log.Printf("write output file failed: %v", err)
						return
					}
				}
			},
		}
		cmdMock.Flags().StringVarP(&output, "output", "o", "bgpnotification.csv", "output file path")
		cmdMock.Flags().IntVarP(&num, "num", "n", 10, "number of record you want to generate")

		var cmdAlter = &cobra.Command{
			Use:   "alter",
			Short: "alter table",
			Run: func(cmd *cobra.Command, args []string) {
				db := storage.NewDB()
				db.AlterBGPNotification()
			},
		}

		var cmdDel = &cobra.Command{
			Use: "del",
			Run: func(cmd *cobra.Command, args []string) {
				db := storage.NewDB()
				db.DelBGPNotification()
			},
		}

		rootCmd.AddCommand(cmdBGP)
		cmdBGP.AddCommand(cmdCreate)
		cmdBGP.AddCommand(cmdDesc)
		cmdBGP.AddCommand(cmdImport)
		cmdBGP.AddCommand(cmdMock)
		cmdBGP.AddCommand(cmdQuery)
		cmdBGP.AddCommand(cmdPut)
		cmdBGP.AddCommand(cmdAlter)
		cmdBGP.AddCommand(cmdDel)
	}
	flag.Parse()
	rootCmd.Execute()
}
