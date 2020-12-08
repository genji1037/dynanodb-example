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
	defaultBGPPayload    = `[{"conversation":"%s","time":"2020-12-04T11:08:17.658Z","data":{"text":"CiRiZWFmZWU3OC1mYWY1LTRjYmMtYTU5YS0yYmU0YTQxNzNhMDkSAwoBMQ==","asset":{"avatar_key":"","name":"bg41707"},"sender":"fdbd3a29681fdade"},"eid":"c34abd7b-7b7b-4dae-8076-d2e677b2c076","from":"a305b070-a9b4-44e7-8aea-113ad4f9aa6c","convtype":5,"type":"conversation.bgp-message-add"}]`
	defaultNotifyPayload = `[{"conversation":"38a201f2-f114-418c-bad1-9b15f6603edd","time":"2020-12-03T04:51:59.083Z","data":{"text":"owABAaEAWCCMbj6botIPi4eH9ke5VMnV1VM7tpGDaCkTQf0M9l1/QAJYvwKkAAABoQBYIJivPRp08G+uiOPpfu1GaPNR4r1DFh1J7GZ5do+akCSEAqEAoQBYIASbSh/h0KJJCQfx9t0wsGC4xchUy7p48VMxmUS/YoF1A6UAUM+BLzrvFruUFViNB7npCeUBAAIAA6EAWCDd3wO27xM2RhOYiVOzmy8E2ks1sqgI72E7jsd0saPkdwRYL4SZHiC3wbHuNP231HQn3Mfldv2U25yHzk9SvSe+81FL1u2DUBMMODScD+GWX8Z5","sender":"12dea3989ffc6720","recipient":"fdbd3a29681fdade"},"eid":"26ce1537-3d62-4e57-a934-3fb9a89e7d21","from":"959c5182-4760-4ca5-afe7-89db21669471","convtype":2,"type":"conversation.otr-message-add"}]`
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
					wg.Wait()
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
				db.PutBGPNotification(storage.BGPNotification{
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
				nCh := make(chan storage.BGPNotification)
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
						tmp := strings.Split(string(line), "|")
						if len(tmp) != 3 {
							log.Printf("bad input file format %s", string(line))
						}
						nCh <- storage.BGPNotification{
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
					go func(n storage.BGPNotification) {
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
					payload := fmt.Sprintf(defaultBGPPayload, cnvID)
					_, err := f.WriteString(fmt.Sprintf("%s|%d|%s\n", cnvID, nID, payload))
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

	// nft sub command
	{
		var input, output string
		var concurrency int64
		var num int

		var cmdNTF = &cobra.Command{
			Use:   "ntf",
			Short: "notification",
		}

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
					userID := cnvs[rand.Intn(len(cnvs))]
					nID := epoch + int64(i)
					payload := defaultNotifyPayload
					_, err := f.WriteString(fmt.Sprintf("%s|%d|%s\n", userID, nID, payload))
					if err != nil {
						log.Printf("write output file failed: %v", err)
						return
					}
				}
			},
		}
		cmdMock.Flags().StringVarP(&output, "output", "o", "notification.csv", "output file path")
		cmdMock.Flags().IntVarP(&num, "num", "n", 10, "number of record you want to generate")

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
						tmp := strings.Split(string(line), "|")
						if len(tmp) != 3 {
							log.Printf("bad input file format %s", string(line))
						}
						nCh <- storage.Notification{
							UserID:  tmp[0],
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
						db.PutNotification(n)
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

		rootCmd.AddCommand(cmdNTF)
		cmdNTF.AddCommand(cmdMock)
		cmdNTF.AddCommand(cmdImport)
	}

	flag.Parse()
	rootCmd.Execute()
}
