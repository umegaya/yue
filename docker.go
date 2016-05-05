package yue

import (
	"log"
	"os"
	"net/http"
	"sync"
	"path/filepath"

	docker "github.com/docker/engine-api/client"
	"github.com/docker/go-connections/tlsconfig"
	"github.com/docker/engine-api/types"

	"golang.org/x/net/context"
)

//docker controller supports docker remote api request to multiple node in same cluster, which uses same access settings.
type dockerctrl struct {
	clients map[string]*docker.Client
	myclient *docker.Client
	hc *http.Client
	apiVersion string
	mutex sync.RWMutex
}

func newdockerctrl() *dockerctrl {
	return &dockerctrl {
		clients: make(map[string]*docker.Client),
		mutex: sync.RWMutex{},
	}
}

func (dc *dockerctrl) init(c *Config) error {
	path := c.DockerCertPath
	log.Printf("DockerCertPath:%s", c.DockerCertPath)
	options := tlsconfig.Options{
		CAFile:             filepath.Join(path, "ca.pem"),
		CertFile:           filepath.Join(path, "cert.pem"),
		KeyFile:            filepath.Join(path, "key.pem"),
		InsecureSkipVerify: len(path) == 0,
	}
	tlsc, err := tlsconfig.Client(options)
	if err != nil {
		return err
	}
	dc.hc = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsc,
		},
	}
	if len(c.DefaultApiVersion) > 0 {
		dc.apiVersion = c.DefaultApiVersion
	} else {
		dc.apiVersion = os.Getenv("DOCKER_API_VERSION")
	}
	//database should run in same docker host
	dc.myclient = dc.ensureGet(c.DatabaseAddress)
	return nil
}

func (dc *dockerctrl) stop(ctx context.Context, host, id_or_name string) {
	client := dc.ensureGet(host)
	log.Print("stop and cleanup container:", id_or_name)
	client.ContainerKill(ctx, id_or_name, "")
	client.ContainerRemove(ctx, types.ContainerRemoveOptions{
		ContainerID: id_or_name,
		RemoveVolumes: true,
		Force:         true,			
	})
}

func (dc *dockerctrl) get(host string) *docker.Client {
	defer dc.mutex.RUnlock()
	dc.mutex.RLock()
	return dc.clients[host]
}

func (dc *dockerctrl) ensureGet(host string) *docker.Client {
	var err error
	d := dc.get(host)
	if d != nil {
		return d
	}
	d, err = docker.NewClient(host, dc.apiVersion, dc.hc, nil)
	if err != nil {
		return nil
	}
	defer dc.mutex.Unlock()
	dc.clients[host] = d
	return d
}
