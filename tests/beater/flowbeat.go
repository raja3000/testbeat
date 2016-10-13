package beater

// #include <sys/file.h>
import "C"

import (
	"fmt"
	"time"
    "os"
    "bufio"
    "strings"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/elastic/beats/flowbeat/config"
)

type Flowbeat struct {
	done   chan struct{}
	config config.Config
	client publisher.Client
    lastIndexTime time.Time
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Flowbeat{
		done: make(chan struct{}),
		config: config,
	}
	return bt, nil
}

func (bt *Flowbeat) Run(b *beat.Beat) error {
	logp.Info("flowbeat is running! Hit CTRL-C to stop it.")

    /* 
     * Open the file 
     */
   
    fmt.Println("Configured Period : ", int(bt.config.Period), " Path ", bt.config.Path)

    bt.client = b.Publisher.Connect()

    /*
    fi, err := os.Open(bt.config.Path)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

    defer fi.Close()
    fd := fi.Fd()
    */

    ticker := time.NewTicker(bt.config.Period)

    for {

		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

        time.Sleep(bt.config.Period)

        fi, err := os.Open(bt.config.Path)
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }

        fd := fi.Fd()

        finfo, err := os.Stat(bt.config.Path)
        if err != nil {
            fmt.Println(err)
            continue
        }

        t := finfo.ModTime()

        if (!t.After(bt.lastIndexTime)) {
            fmt.Println("LastIndexTime %u, ModTime %u", bt.lastIndexTime, t)
            continue
        }

        returnCode := C.flock(C.int(fd), C.LOCK_EX)

        if returnCode < 0 {
            fmt.Println("Flock lock returned bad status: %d", returnCode)
            continue
        }

        scanner := bufio.NewScanner(fi)

        for scanner.Scan() {

            fmt.Println(scanner.Text())
            result := strings.Split(scanner.Text(), ",")

		    event := common.MapStr{
			    "@timestamp": common.Time(time.Now()),
			    "type":       b.Name,
                "srcip":      result[0],
                "dstip":      result[1],
                "srcport":    result[2],
                "dstport":    result[3],
                "proto":      result[4],
                "packets":    result[5],
                "bytes":      result[6],
                "group":      result[7],

		     }

		     retBool := bt.client.PublishEvent(event, 
                    publisher.Guaranteed)

             if (retBool == false) {
                fmt.Println("Could not send data ")
             }
        }

        err = fi.Truncate(0) 
        if (err != nil) {
		    fmt.Errorf("Error truncating file: %v", err)
        }

        fi.Sync()

        bt.lastIndexTime = time.Now()
        
        os.Truncate(bt.config.Path, 0)

        returnCode = C.flock(C.int(fd), C.LOCK_UN)

        if returnCode < 0 {
            fmt.Println("Flock unlock returned bad status: %d", returnCode)
            continue 
        }


        fi.Close()
    }
}

func (bt *Flowbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

