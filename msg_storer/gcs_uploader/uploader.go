package uploader

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/weedbox/common-modules/nats_connector"
	"github.com/weedbox/gcp-modules/bucket_connector"
)

const (
	DefaultDomain         = "onglai-msg"
	DefaultSubject        = "%s.archive.bucket.job.%s"
	DefaultBucketName     = "example.com"
	DefaultBucketCategory = "msg-store"
)

type Uploader struct {
	params         Params
	logger         *zap.Logger
	scope          string
	domain         string
	bucketName     string
	bucketCategory string
	hostname       string
}

type Params struct {
	fx.In
	NATSConnector   *nats_connector.NATSConnector
	BucketConnector *bucket_connector.BucketConnector
	Lifecycle       fx.Lifecycle
	Logger          *zap.Logger
}

func Module(scope string) fx.Option {

	var u *Uploader

	return fx.Options(
		fx.Provide(func(p Params) *Uploader {

			u = &Uploader{
				params: p,
				logger: p.Logger.Named(scope),
				scope:  scope,
			}
			u.initDefaultConfigs()
			return u
		}),
		fx.Populate(&u),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: u.onStart,
					OnStop:  u.onStop,
				},
			)
		}),
	)

}

func (u *Uploader) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", u.scope, key)
}

func (u *Uploader) initDefaultConfigs() {
	viper.SetDefault(u.getConfigPath("archive_domain"), DefaultDomain)
	viper.SetDefault(u.getConfigPath("bucket_name"), DefaultBucketName)
	viper.SetDefault(u.getConfigPath("bucket_category"), DefaultBucketCategory)
}

func (u *Uploader) onStart(ctx context.Context) error {

	u.logger.Info("Starting Uploader")

	u.domain = viper.GetString(u.getConfigPath("archive_domain"))
	u.bucketName = viper.GetString(u.getConfigPath("bucket_name"))
	u.bucketCategory = viper.GetString(u.getConfigPath("bucket_category"))

	//get hostname
	hostname, err := os.Hostname()
	if err != nil {
		u.logger.Fatal(err.Error())
	}
	u.hostname = hostname

	err = u.startSubscriber()
	if err != nil {
		return err
	}

	return nil
}

func (u *Uploader) onStop(ctx context.Context) error {
	u.logger.Info("Stopped Uploader")

	return nil
}

func (u *Uploader) startSubscriber() error {
	// nats stream pub a msg to cloud-uploader
	js := u.params.NATSConnector.GetJetStreamContext()
	subject := fmt.Sprintf(DefaultSubject, u.domain, u.hostname)
	go func() {
		//u.logger.Info(subject)
		_, err := js.Subscribe(subject,
			u.msgHandler,
			nats.ManualAck(),
		)
		if err != nil {
			u.logger.Fatal(err.Error())
		}
	}()
	return nil
}

func (u *Uploader) updateIndex(filename string, archiveName string, seq string) error {

	// prepare data
	data := fmt.Sprintf("%s:%s\n", seq, archiveName)

	// opend index file
	dstDir := path.Dir(filename)
	indexFilename := path.Join(dstDir, "archive.index")
	indexFile, err := os.OpenFile(indexFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	// write index file
	_, err = indexFile.WriteString(data)
	if err != nil {
		return err
	}
	return nil
}

func (u *Uploader) msgHandler(m *nats.Msg) {
	mdata := strings.SplitN(string(m.Data), ":", 2)
	archiveFilename := mdata[1]

	//read file
	data, err := os.ReadFile(archiveFilename)
	if err != nil {
		if os.IsNotExist(err) {
			u.logger.Debug(err.Error())
			u.logger.Debug("Skip ...")

			m.Ack()
			return
		}
		m.Nak()
		u.logger.Error(err.Error())
		return
	}

	//prepare upload request
	uploadReq := bucket_connector.UploaderReq{
		FileName: fmt.Sprintf("%s", archiveFilename),
		Category: u.bucketCategory,
		RawData:  string(data),
	}

	// upload
	url, err := u.saveFile(&uploadReq)

	if err != nil {
		m.Nak()
		u.logger.Error(err.Error())
		return
	}

	//update indexFile
	err = u.updateIndex(archiveFilename, url, mdata[0])
	if err != nil {
		m.Nak()
		u.logger.Error(err.Error())
		return
	}

	// remove file
	err = os.RemoveAll(archiveFilename)

	if err != nil {
		m.Nak()
		u.logger.Error(err.Error())
		return
	}

	m.Ack()
}

func (u *Uploader) saveFile(req *bucket_connector.UploaderReq) (string, error) {
	// new a bucket client
	ctx := context.Background()

	reader := strings.NewReader(req.RawData)

	// init uploder
	fileName := req.FileName

	filePath := fmt.Sprintf("%s/%s", req.Category, fileName)

	bucket := u.params.BucketConnector.GetClient().Bucket(u.bucketName)
	w := bucket.Object(filePath).NewWriter(ctx)
	w.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}

	// upload to bucket
	if _, err := io.Copy(w, reader); err != nil {
		u.logger.Error("io.Copy Error")
		return "", err
	}
	if err := w.Close(); err != nil {
		u.logger.Error("io.Close Error")
		return "", err
	}

	resultUrl, err := url.Parse(fmt.Sprintf("%v/%v", w.Attrs().Bucket, w.Attrs().Name))
	if err != nil {
		u.logger.Error("url.Parse Error")
		return "", err
	}

	url := fmt.Sprintf("https://%s", resultUrl.EscapedPath())

	return url, nil
}
