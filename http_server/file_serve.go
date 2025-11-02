package http_server

import (
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (hs *HTTPServer) ServeFS(routePath string, fst fs.FS) {

	// Normalize routePath: ensure it starts with "/" and doesn't end with "/"
	if routePath != "" && !strings.HasPrefix(routePath, "/") {
		routePath = "/" + routePath
	}
	routePath = strings.TrimSuffix(routePath, "/")

	hs.router.NoRoute(func(c *gin.Context) {

		requestPath := c.Request.URL.Path

		// If routePath is set, check if the request path starts with it
		if routePath != "" {
			if !strings.HasPrefix(requestPath, routePath) {
				// Not for this file server, skip
				return
			}
			// Remove the routePath from the request path
			requestPath = strings.TrimPrefix(requestPath, routePath)
		}

		p := strings.TrimPrefix(path.Clean(requestPath), "/")
		if p == "" || strings.HasSuffix(requestPath, "/") {
			serveIndex(c, fst)
			return
		}

		if ok := serveFile(c, fst, p); ok {
			return
		}

		// fallback to index.html
		serveIndex(c, fst)
	})
}

func serveIndex(c *gin.Context, fst fs.FS) {
	data, err := fs.ReadFile(fst, "index.html")
	if err != nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", data)
}

// serveFile streams a file without any redirect logic.
func serveFile(c *gin.Context, fst fs.FS, p string) bool {

	f, err := fst.Open(p)
	if err != nil {
		return false
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || info.IsDir() {
		return false
	}

	if ct := mime.TypeByExtension(filepath.Ext(p)); ct != "" {
		c.Header("Content-Type", ct)
	}

	http.ServeContent(c.Writer, c.Request, p, modTime(info), f.(io.ReadSeeker))
	return true
}

func modTime(fi fs.FileInfo) time.Time {
	if fi.ModTime().IsZero() {
		return time.Unix(0, 0)
	}
	return fi.ModTime()
}
