package msg_storer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/weedbox/common-modules/nats_connector"
)

const (
	DefaultCurrentDB    = "current.db"
	DefaultArchiveIndex = "archive.index"
	DefaultDatastore    = "./datastore"
	fileSize            = 1024 * 1024 * 1 //1MB unit: Bytes
	DefaultDomain       = "onglai-msg"
	DefaultSubject      = "%s.archive.bucket.job.%s"
)

var (
	ErrSeqNotFound = errors.New("Sequence not fount in the index.")
)

type Storer struct {
	params    Params
	logger    *zap.Logger
	scope     string
	datastore string
	counter   uint64
	domain    string
	hostname  string
}

type Params struct {
	fx.In
	NATSConnector *nats_connector.NATSConnector
	Lifecycle     fx.Lifecycle
	Logger        *zap.Logger
}

func Module(scope string) fx.Option {

	var sr *Storer

	return fx.Options(
		fx.Provide(func(p Params) *Storer {

			sr = &Storer{
				params: p,
				logger: p.Logger.Named(scope),
				scope:  scope,
			}
			sr.initDefaultConfigs()
			return sr
		}),
		fx.Populate(&sr),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: sr.onStart,
					OnStop:  sr.onStop,
				},
			)
		}),
	)

}

func (sr *Storer) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", sr.scope, key)
}

func (sr *Storer) initDefaultConfigs() {
	viper.SetDefault(sr.getConfigPath("datastore"), DefaultDatastore)
	viper.SetDefault(sr.getConfigPath("archive_domain"), DefaultDomain)
}

func (sr *Storer) onStart(ctx context.Context) error {

	sr.logger.Info("Starting Storer")

	sr.datastore = viper.GetString(sr.getConfigPath("datastore"))
	sr.domain = viper.GetString(sr.getConfigPath("archive_domain"))

	sr.counter = uint64(0)

	//get hostname
	hostname, err := os.Hostname()
	if err != nil {
		sr.logger.Fatal(err.Error())
	}
	sr.hostname = hostname

	// create stream.
	js := sr.params.NATSConnector.GetJetStreamContext()
	_, err = js.AddStream(
		&nats.StreamConfig{
			Name:       fmt.Sprintf("%s_Archive_Job", sr.domain),
			Subjects:   []string{fmt.Sprintf(DefaultSubject, sr.domain, ">")},
			Retention:  nats.WorkQueuePolicy,
			Storage:    nats.FileStorage,
			Replicas:   1,
			Discard:    nats.DiscardOld,
			MaxMsgs:    -1,
			MaxBytes:   -1,
			MaxAge:     0,
			MaxMsgSize: -1,
			Duplicates: time.Second * 120,
		})
	if err != nil {
		sr.logger.Fatal(err.Error())
	}

	return nil
}

func (sr *Storer) onStop(ctx context.Context) error {
	sr.logger.Info("Stopped Storer")

	return nil
}

func (sr *Storer) GetArchivedFileBySeq(dstPath string, seq uint64) (string, error) {

	dstDir := path.Join(sr.datastore, dstPath)

	//check from current file
	currentFile := path.Join(dstDir, DefaultCurrentDB)
	seqStr, err := sr.getFirstSeqFromFile(currentFile)
	if err != nil {
		return "", err
	}
	curSeq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return "", err
	}

	if seq >= curSeq {
		return currentFile, nil
	}

	// search archived url/path by seq
	// read index file
	dstFile := path.Join(dstDir, DefaultArchiveIndex)
	fr, err := os.Open(dstFile)
	if err != nil {
		return "", err
	}
	defer fr.Close()

	// new scanner
	scanner := bufio.NewScanner(fr)

	// scan
	afile := ""
	for scanner.Scan() {
		parseData := strings.SplitN(scanner.Text(), ":", 2)
		archiveSeq, err := strconv.ParseUint(parseData[0], 10, 64)
		if err != nil {
			sr.logger.Error(err.Error())
			continue
		}
		if seq >= archiveSeq {
			afile = parseData[1]
		} else {
			break
		}
	}

	if afile != "" {

		return afile, nil
	}

	return "", ErrSeqNotFound

}

func (sr *Storer) MsgStore(dstPath string, seq uint64, rawData []byte) (string, error) {

	dstFile, err := sr.processFilename(dstPath)
	if err != nil {
		sr.logger.Debug(fmt.Sprintf("process filename error: %v", err.Error()))
		return "", err
	}

	var dst *os.File
	for {
		// open file and append data
		dst, err = os.OpenFile(dstFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			sr.logger.Debug(fmt.Sprintf("open file error: %v", err.Error()))
			return "", err
		}
		defer dst.Close()

		//Check every 100 transactions
		if sr.counter < 100 {
			// skip
			sr.counter = sr.counter + uint64(1)
			break
		}

		sr.counter = 0

		// check fileSize
		fi, err := dst.Stat()
		if err != nil {
			sr.logger.Debug(fmt.Sprintf("get file size error: %v", err.Error()))
			return "", err
		}

		//c.logger.Info(fmt.Sprintf("%d", fi.Size()))
		if fi.Size() >= fileSize {
			dst.Close()

			// archive old file
			err := sr.archiveFile(dstFile)
			if err != nil {
				sr.logger.Debug(fmt.Sprintf("archive file error: %v", err.Error()))
				return "", err
			}

			continue
		}
		break
	}

	data := fmt.Sprintf("%d:%s\n", seq, string(rawData))
	_, err = dst.WriteString(data)
	if err != nil {
		sr.logger.Debug(fmt.Sprintf("write data error: %v", err.Error()))
		return "", err
	}

	//sr.logger.Debug(fmt.Sprintf("write data to: %v, seq: %d", dstFile, seq))

	return dstFile, nil
}

func (sr *Storer) getFirstSeqFromFile(filename string) (string, error) {
	// open file
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// new reader
	reader := bufio.NewReader(file)

	// read first line
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	cols := strings.SplitN(line, ":", 2)

	return cols[0], nil
}

func (sr *Storer) updateIndex(filename string, archiveName string, seq string) error {

	//prepare data
	data := fmt.Sprintf("%s:%s\n", seq, archiveName)

	// open index file
	dstDir := path.Dir(filename)
	indexFilename := path.Join(dstDir, DefaultArchiveIndex)
	indexFile, err := os.OpenFile(indexFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	// write to index file
	_, err = indexFile.WriteString(data)
	if err != nil {
		return err
	}
	return nil
}

func (sr *Storer) archiveFile(filename string) error {

	//get first seq from current db
	seq, err := sr.getFirstSeqFromFile(filename)
	if err != nil {
		return err
	}

	if os.Getenv("TEST_MODE") != "" {

		archivePath := strings.ReplaceAll(filename, path.Join(sr.datastore), path.Join(sr.datastore, "archive"))
		archiveName := path.Join(path.Dir(archivePath), fmt.Sprintf("MSG_%s.db", seq))

		err := os.MkdirAll(path.Dir(archiveName), 0750)
		if err != nil {
			return err
		}

		sr.logger.Debug("Archive file",
			zap.String("fileName", filename),
			zap.String("archiveName", archiveName),
		)

		if err := os.Rename(filename, archiveName); err != nil {
			return err
		}

		// update indexfile
		return sr.updateIndex(filename, archiveName, seq)

	} else {
		// this seq is first seq from current db
		archiveName := path.Join(path.Dir(filename), fmt.Sprintf("MSG_%s.db", seq))
		if err := os.Rename(filename, archiveName); err != nil {
			return err
		}
		sr.logger.Debug(fmt.Sprintf("%s move to  %s", filename, archiveName))

		// upload to bucket storage (gcs)
		err := sr.triggerUploader(archiveName, seq)
		if err != nil {
			return err
		}

		sr.logger.Debug(fmt.Sprintf("trigger uploader to upload : %v", archiveName))
		// update indexfile from plugin

		return nil

	}

}

func (sr *Storer) processFilename(dstPath string) (string, error) {
	// create dir
	dstDir := path.Join(sr.datastore, dstPath)
	err := os.MkdirAll(dstDir, 0750)
	if err != nil {
		return "", err
	}

	dstFile := path.Join(dstDir, DefaultCurrentDB)

	return dstFile, nil
}

func (sr *Storer) triggerUploader(filename string, seq string) error {

	// nats stream pub a msg to cloud-uploader
	js := sr.params.NATSConnector.GetJetStreamContext()
	subject := fmt.Sprintf(DefaultSubject, sr.domain, sr.hostname)

	data := fmt.Sprintf("%s:%s", seq, filename)

	for {
		_, err := js.Publish(subject, []byte(data), nats.MsgId(data))
		if err != nil {
			sr.logger.Error(subject)
			sr.logger.Error(err.Error())
			sr.logger.Error("retry ...")

			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}

	return nil
}
