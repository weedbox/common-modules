package uploader

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/suite"
	"github.com/weedbox/common-modules/configs"
	"github.com/weedbox/common-modules/daemon"
	"github.com/weedbox/common-modules/logger"
	"github.com/weedbox/common-modules/nats_connector"
	"github.com/weedbox/gcp-modules/bucket_connector"
	"go.uber.org/fx"
)

func runNatsServer() *server.Server {
	// jetstream server
	sdir := fmt.Sprintf("%s", "nats_datastore")
	opts := server.Options{
		Host:          "127.0.0.1",
		Port:          32803,
		Debug:         false,
		MaxPayload:    1024 * 1024 * 32,
		WriteDeadline: 10 * time.Second,
		JetStream:     true,
		ServerName:    "nats-tester",
		StoreDir:      sdir,
	}

	// Run server
	ser, err := server.NewServer(&opts)
	if err != nil {
		log.Fatal(err)
	}

	// Run nats server
	err = server.Run(ser)
	if err != nil {
		log.Fatal(err)
	}

	return ser
}

func getUploader() *Uploader {
	config := configs.NewConfig("SERVICE")

	var u *Uploader
	app := fx.New(
		fx.Supply(config),

		// Modules
		logger.Module(),
		nats_connector.Module("internal_event"),
		bucket_connector.Module("bucket"),

		// uploader
		fx.Provide(func(p Params) *Uploader {

			u = &Uploader{
				params: p,
				logger: p.Logger.Named("uploader"),
				scope:  "uploader",
			}
			u.initDefaultConfigs()
			u.domain = DefaultDomain
			u.bucketName = "fkdata"
			u.bucketCategory = DefaultBucketCategory

			return u
		}),
		fx.Populate(&u),

		// Integration
		daemon.Module("daemon"),
		fx.NopLogger,
	)
	ctx := context.Background()
	app.Start(ctx)
	//defer app.Stop(ctx)

	//get hostname
	hostname, err := os.Hostname()
	if err != nil {
		u.logger.Fatal(err.Error())
	}
	u.hostname = hostname

	// create stream.
	js := u.params.NATSConnector.GetJetStreamContext()
	_, err = js.AddStream(
		&nats.StreamConfig{
			Name:       fmt.Sprintf("%s_Archive_Job", u.domain),
			Subjects:   []string{fmt.Sprintf(DefaultSubject, u.domain, "*")},
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
		log.Fatal(err)
	}

	// create test index file.
	testIndex := "datastore/100/100/archive.index"
	err = os.MkdirAll(path.Dir(testIndex), 0750)
	if err != nil {
		log.Fatal(err)
	}
	indexFile, err := os.OpenFile(testIndex, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	indexFile.Close()

	testFile := "datastore/100/100/MSG_99999.db"
	tFile, err := os.OpenFile(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	tFile.Close()

	return u

}

type TestSuite struct {
	suite.Suite
	uploader        *Uploader
	server          *server.Server
	natsMsg         *nats.Msg
	currentFilename string
}

func TestMain(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func (s *TestSuite) SetupSuite() {
	server := runNatsServer()
	for {
		if server.ReadyForConnections(100 * time.Millisecond) {
			s.T().Log("NATS Server starting")
			break
		}
		s.T().Log("Waitting for NATS Server starting ...")

	}
	s.server = server

	s.uploader = getUploader()
}

func (s *TestSuite) TearDownSuite() {
	// clear test data
	err := os.RemoveAll("./datastore")
	if err != nil {
		fmt.Println("Error cleaning up test data:", err)
	}

	// clear test data
	err = os.RemoveAll("./nats_datastore")
	if err != nil {
		fmt.Println("Error cleaning up test data:", err)
	}
}

func (s *TestSuite) TestStartSubscriber() {
	u := s.uploader
	exp := "99999:datastore/100/100/MSG_99999.db"

	//subscribe
	js := u.params.NATSConnector.GetJetStreamContext()
	var wg sync.WaitGroup
	wg.Add(1)
	subject := fmt.Sprintf(DefaultSubject, u.domain, u.hostname)
	go func() {
		// 在这里模拟 QueueSubscribe 和消息处理的逻辑
		//s.T().Log("subscribe subject: ", subject)
		_, err := js.Subscribe(subject,
			func(m *nats.Msg) {
				s.Equal(exp, string(m.Data), "result should be %s", exp)

				s.natsMsg = m
				m.Ack()
				wg.Done()
			},
			nats.ManualAck(),
		)
		if err != nil {
			s.T().Errorf("Error in QueueSubscribe: %v", err)
		}

	}()

	// 模拟消息处理
	js.Publish(subject, []byte(exp))

	// wait
	wg.Wait()
}

func (s *TestSuite) TestZMsgHandler() {
	u := s.uploader
	m := s.natsMsg

	mdata := strings.SplitN(string(m.Data), ":", 2)
	filename := mdata[1]

	//read file
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		s.Fail(err.Error())
	}

	//prepare upload request
	uploadReq := bucket_connector.UploaderReq{
		FileName: fmt.Sprintf("%s", filename),
		Category: u.bucketCategory,
		RawData:  string(data),
	}

	// upload
	url, err := u.saveFile(&uploadReq)

	if err != nil {
		s.Fail(err.Error())
	}

	//update indexFile
	err = u.updateIndex(filename, url, mdata[0])
	if err != nil {
		s.Fail(err.Error())
	}

	// remove file
	err = os.RemoveAll(filename)

	if err != nil {
		s.Fail(err.Error())
	}

}

func (s *TestSuite) TestZUpdateIndex() {
	u := s.uploader
	filename := "datastore/100/100/MSG_99999.db"
	archivename := "http://localhost/datastore/100/100/MSG_99999.db"

	err := u.updateIndex(filename, archivename, "99999")

	if err != nil {
		s.Fail(err.Error())
	}

	// check content
	dstDir := path.Dir(filename)
	indexFilename := path.Join(dstDir, "archive.index")
	fr, err := os.Open(indexFilename)

	if err != nil {
		s.Fail(err.Error())
	}

	defer fr.Close()

	// new scanner
	scanner := bufio.NewScanner(fr)

	// get last line
	var lastLine string

	for scanner.Scan() {
		lastLine = scanner.Text()
	}

	expected := fmt.Sprintf("%d:%s", 99999, archivename)
	s.Equal(expected, lastLine, "Last line should be %s", expected)
}

func BenchmarkUpdateIndex(b *testing.B) {
	u := getUploader()
	filename := "datastore/100/100/MSG_%d.db"
	archivename := "http://localhost/datastore/100/100/MSG_%d.db"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstDir := fmt.Sprintf(filename, i)
		archivePath := fmt.Sprintf(archivename, i)
		seq := fmt.Sprintf("%d", i)
		err := u.updateIndex(dstDir, archivePath, seq)
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
}
