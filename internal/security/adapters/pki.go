package adapters

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/nickemma/meridian/internal/security/domain"
)

// GenerateDevelopmentPKI creates a new directory only. These short-lived keys
// support local experiments, not production credential provisioning.
func GenerateDevelopmentPKI(dir string) error {
	return generateDevelopmentPKI(dir, time.Now())
}

func generateDevelopmentPKI(dir string, now time.Time) error {
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial := func() (*big.Int, error) { return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128)) }
	n, err := serial()
	if err != nil {
		return err
	}
	ca := &x509.Certificate{SerialNumber: n, Subject: pkix.Name{CommonName: "Meridian local research CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(48 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	write := func(name, kind string, data []byte) error {
		return os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), 0600)
	}
	if err := write("ca.pem", "CERTIFICATE", der); err != nil {
		return err
	}
	// The issuer's private key is intentionally not saved. Regenerate a complete
	// local bundle when it expires; operational issuance/rotation is future work.
	for _, id := range []string{"node-1", "node-2", "node-3", "tenant-a", "tenant-a-reader", "tenant-b", "unregistered"} {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		n, err := serial()
		if err != nil {
			return err
		}
		role := domain.ClientPrefix
		if id == "node-1" || id == "node-2" || id == "node-3" {
			role = domain.NodePrefix
		}
		uri, err := url.Parse(role + id)
		if err != nil {
			return err
		}
		cert := &x509.Certificate{SerialNumber: n, Subject: pkix.Name{CommonName: id}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri}}
		if role == domain.NodePrefix {
			cert.ExtKeyUsage = append(cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
			cert.DNSNames = []string{id, "localhost"}
			cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if err != nil {
			return err
		}
		if err := write(id+".pem", "CERTIFICATE", der); err != nil {
			return err
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return err
		}
		if err := write(id+".key", "PRIVATE KEY", keyDER); err != nil {
			return err
		}
	}
	rules := []domain.Rule{{Identity: "tenant-a", Tenant: "a", Read: true, Write: true}, {Identity: "tenant-a-reader", Tenant: "a", Read: true}, {Identity: "tenant-b", Tenant: "b", Read: true, Write: true}}
	if err := writeRules(filepath.Join(dir, "clients.json"), rules); err != nil {
		return fmt.Errorf("write policies: %w", err)
	}
	return nil
}
