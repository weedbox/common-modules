package msg_storer

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

func getStorer() *Storer {
	config := configs.NewConfig("SERVICE")

	var sr *Storer
	app := fx.New(
		fx.Supply(config),

		// Modules
		logger.Module(),
		nats_connector.Module("internal_event"),

		// storer
		fx.Provide(func(p Params) *Storer {

			sr = &Storer{
				params: p,
				logger: p.Logger.Named("storer"),
				scope:  "storer",
			}
			sr.initDefaultConfigs()
			sr.counter = uint64(0)
			sr.datastore = DefaultDatastore
			sr.domain = DefaultDomain
			return sr
		}),
		fx.Populate(&sr),

		// Integration
		daemon.Module("daemon"),
		fx.NopLogger,
	)
	ctx := context.Background()
	app.Start(ctx)
	//defer app.Stop(ctx)
	// create stream.
	js := sr.params.NATSConnector.GetJetStreamContext()
	_, err := js.AddStream(
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
		log.Fatal(err)
	}

	//get hostname
	hostname, err := os.Hostname()
	if err != nil {
		sr.logger.Fatal(err.Error())
	}
	sr.hostname = hostname

	return sr

}

type TestSuite struct {
	suite.Suite
	storer          *Storer
	server          *server.Server
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

	s.storer = getStorer()
}

func (s *TestSuite) TearDownSuite() {
	// clear test data
	err := os.RemoveAll("./datastore")
	if err != nil {
		fmt.Println("Error cleaning up test data:", err)
	}

	err = os.RemoveAll("./nats_datastore")
	if err != nil {
		fmt.Println("Error cleaning up test data:", err)
	}
}

func (s *TestSuite) TestProcessFilename() {
	sr := s.storer
	dstPath := "100/100"

	filename, err := sr.processFilename(dstPath)
	if err != nil {
		s.Fail(err.Error())
	}

	expected := path.Join(sr.datastore, "100/100", DefaultCurrentDB)
	s.Equal(expected, filename, "filename should be %s", expected)
}

func (s *TestSuite) TestMsgStore() {
	sr := s.storer

	data := []byte("test MsgStore function")

	dstPath := "100/100"

	seq := uint64(0)

	dstFile, err := sr.MsgStore(dstPath, seq, data)
	if err != nil {
		s.Fail(err.Error())
	}

	// check content
	fr, err := os.Open(dstFile)
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
	expected := fmt.Sprintf("%d:%s", seq, string(data))
	s.Equal(expected, lastLine, "Last line should be %s", expected)
	s.currentFilename = dstFile
}

func (s *TestSuite) TestMsgStoreArchive() {
	sr := s.storer
	sr.datastore = DefaultDatastore
	err := sr.archiveFile(s.currentFilename)
	if err != nil {
		s.Fail(err.Error())
	}

	//check file already archive
	archivePath := strings.ReplaceAll(s.currentFilename, path.Join(sr.datastore), path.Join(sr.datastore, "archive"))
	archiveFile := path.Join(path.Dir(archivePath), fmt.Sprintf("MSG_%s.db", "0"))

	fr, err := os.Open(archiveFile)
	if err != nil {
		s.Fail(err.Error())
	}
	defer fr.Close()

}

func (s *TestSuite) TestZUpdateIndex() {
	sr := s.storer
	filename := "datastore/100/100/MSG_99999.db"
	archivename := "http://localhost/datastore/100/100/MSG_99999.db"

	err := sr.updateIndex(filename, archivename, "99999")
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

func (s *TestSuite) TestZGetArchivedFileBySeq() {
	sr := s.storer

	data := []byte("test MsgStore function")

	dstPath := "100/100"

	seq := uint64(9999)

	_, err := sr.MsgStore(dstPath, seq, data)
	if err != nil {
		s.Fail(err.Error())
	}

	filename := "datastore/100/100/MSG_%d.db"
	archivename := "http://localhost/datastore/100/100/MSG_%d.db"

	fakeSeqs := []uint64{1001, 2001, 3001}
	for _, fakeSeq := range fakeSeqs {
		err := sr.updateIndex(fmt.Sprintf(filename, fakeSeq), fmt.Sprintf(archivename, fakeSeq), fmt.Sprintf("%d", fakeSeq))
		if err != nil {
			s.Fail(err.Error())
		}
	}

	// seq = 1
	aFile, err := sr.GetArchivedFileBySeq("100/100", uint64(1))
	if err != nil && err != ErrSeqNotFound {
		s.Fail(err.Error())
	}
	expected := "datastore/archive/100/100/MSG_0.db"
	s.Equal(expected, aFile, "archive file should be %s", expected)

	// seq = 1001
	aFile, err = sr.GetArchivedFileBySeq("100/100", uint64(1001))
	if err != nil && err != ErrSeqNotFound {
		s.Fail(err.Error())
	}
	expected = fmt.Sprintf(archivename, 1001)
	s.Equal(expected, aFile, "archive file should be %s", expected)

	// seq = 2022
	aFile, err = sr.GetArchivedFileBySeq("100/100", uint64(2022))
	if err != nil && err != ErrSeqNotFound {
		s.Fail(err.Error())
	}
	expected = fmt.Sprintf(archivename, 2001)
	s.Equal(expected, aFile, "archive file should be %s", expected)

	// seq = 32022
	aFile, err = sr.GetArchivedFileBySeq("100/100", uint64(32022))
	if err != nil && err != ErrSeqNotFound {
		s.Fail(err.Error())
	}
	expected = "datastore/100/100/current.db"
	s.Equal(expected, aFile, "archive file should be %s", expected)

}

func (s *TestSuite) TestTriggerUploader() {
	sr := s.storer
	exp := fmt.Sprintf("%s:%s", "0", s.currentFilename)

	// subscribe for check
	js := sr.params.NATSConnector.GetJetStreamContext()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {

		_, err := js.QueueSubscribe(fmt.Sprintf(DefaultSubject, sr.domain, sr.hostname),
			"msg-store-archive-job-test",
			func(m *nats.Msg) {
				act := m.Data
				s.Equal(exp, string(act), "result should be %s", exp)

				m.Ack()
				wg.Done()
			},
			nats.Durable("msg-store-archive-job-test"),
			nats.ManualAck(),
		)
		if err != nil {
			s.T().Errorf("Error in QueueSubscribe: %v", err)
		}
	}()

	// Trigger
	err := sr.triggerUploader(s.currentFilename, "0")
	if err != nil {
		s.Fail(err.Error())
	}

	wg.Wait()

}

func BenchmarkMsgStore(b *testing.B) {
	sr := getStorer()
	dstPath := "100/100"
	data := []byte("benchmark-test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := sr.MsgStore(dstPath, uint64(i), data)
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
}

func BenchmarkMsgStoreMultiDst(b *testing.B) {
	sr := getStorer()
	dstPath := "100/%d"
	data := []byte("benchmark-test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstPath := fmt.Sprintf(dstPath, i%100)
		_, err := sr.MsgStore(dstPath, uint64(i), data)
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
}

func BenchmarkUpdateIndex(b *testing.B) {
	sr := getStorer()
	filename := "datastore/100/1/MSG_%d.db"
	archivename := "http://localhost/datastore/100/1/MSG_%d.db"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstDir := fmt.Sprintf(filename, i)
		archivePath := fmt.Sprintf(archivename, i)
		seq := fmt.Sprintf("%d", i)
		err := sr.updateIndex(dstDir, archivePath, seq)
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
}
