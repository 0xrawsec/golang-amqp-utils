package amqpconfig

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/0xrawsec/golang-utils/config"
	"github.com/0xrawsec/golang-utils/log"
)

// Config structure for Collector
type Config struct {
	AmqpURL string
	TLSConf tls.Config
	Workers int // Used only by consumer
}

// LoadConfigFromFile loads the configuration from a file
func LoadConfigFromFile(configPath string) (c Config, err error) {
	conf, err := config.Load(configPath)
	if err != nil {
		return
	}
	// Load amqp-url
	if url, err := conf.GetString("amqp-url"); err == nil {
		c.AmqpURL = url
	}
	// Loads the CA cert
	if certPath, err := conf.GetString("ca-cert"); err == nil {
		if err = c.LoadCACert(certPath); err != nil {
			log.Errorf("Error loading CA certificate: %s", err)
		}
	}
	// Loads the client key pair
	if kpSubconfig, err := conf.GetSubConfig("client-keypair"); err == nil {
		// client certificate
		clCert, err := kpSubconfig.GetString("client-cert")
		if err != nil {
			log.Error("Error no client certificate in configuration file")
		} else {
			// client key
			clKey, err := kpSubconfig.GetString("client-key")
			if err != nil {
				log.Error("Error no client key in configuration file")
			}
			if err := c.LoadClientKeyPair(clCert, clKey); err != nil {
				log.Errorf("Failed loading client key pair: %s", err)
			}
		}
	}
	// Init the workers
	if workers, err := conf.GetInt64("workers"); err == nil {
		c.Workers = int(workers)
	}
	return
}

// LoadCACert loads server certificate
func (c *Config) LoadCACert(path string) (err error) {
	var ca []byte
	if c.TLSConf.RootCAs == nil {
		c.TLSConf.RootCAs = x509.NewCertPool()
	}
	if ca, err = ioutil.ReadFile(path); err == nil {
		c.TLSConf.RootCAs.AppendCertsFromPEM(ca)
	}
	return
}

// LoadClientKeyPair loads the client certificate and key
func (c *Config) LoadClientKeyPair(certPath, keyPath string) (err error) {
	var cert tls.Certificate
	if cert, err = tls.LoadX509KeyPair(certPath, keyPath); err == nil {
		c.TLSConf.Certificates = append(c.TLSConf.Certificates, cert)
	}
	return
}

/*
// Config structure for Publisher
type Config struct {
	AmqpURL string
	TLSConf tls.Config
}

// LoadFromFile loads the configuration from a file
func (c *Config) LoadFromFile(configPath string) (err error) {
	conf, err := config.Load(configPath)
	if err != nil {
		return
	}
	// Load amqp-url
	if url, err := conf.GetString("amqp-url"); err == nil {
		c.AmqpURL = url
	}
	// Loads the CA cert
	if certPath, err := conf.GetString("ca-cert"); err == nil {
		if err = c.LoadCACert(certPath); err != nil {
			log.Errorf("Error loading CA certificate: %s", err)
		}
	}
	// Loads the client key pair
	if kpSubconfig, err := conf.GetSubConfig("client-keypair"); err == nil {
		// client certificate
		clCert, err := kpSubconfig.GetString("client-cert")
		if err != nil {
			log.Error("Error no client certificate in configuration file")
		} else {
			// client key
			clKey, err := kpSubconfig.GetString("client-key")
			if err != nil {
				log.Error("Error no client key in configuration file")
			}
			if err := c.LoadClientKeyPair(clCert, clKey); err != nil {
				log.Errorf("Failed loading client key pair: %s", err)
			}
		}
	}
	return
}

// LoadCACert loads server certificate
func (c *Config) LoadCACert(path string) (err error) {
	var ca []byte
	if c.TLSConf.RootCAs == nil {
		c.TLSConf.RootCAs = x509.NewCertPool()
	}
	if ca, err = ioutil.ReadFile(path); err == nil {
		c.TLSConf.RootCAs.AppendCertsFromPEM(ca)
	}
	return
}

// LoadClientKeyPair loads the client certificate and key
func (c *Config) LoadClientKeyPair(certPath, keyPath string) (err error) {
	var cert tls.Certificate
	if cert, err = tls.LoadX509KeyPair(certPath, keyPath); err == nil {
		c.TLSConf.Certificates = append(c.TLSConf.Certificates, cert)
	}
	return
}*/
