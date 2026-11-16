package adapters

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/nickemma/meridian/internal/security/domain"
)

func certificateIdentity(certs []*x509.Certificate) string {
	if len(certs) == 0 || len(certs[0].URIs) != 1 {
		return ""
	}
	return certs[0].URIs[0].String()
}

// loadPair loads a certificate and key, and verifies the chain against the CA
// for client authentication before it can be used on any connection.
func loadPair(caFile, certFile, keyFile string) (tls.Certificate, *x509.CertPool, error) {
	if caFile == "" || certFile == "" || keyFile == "" {
		return tls.Certificate{}, nil, fmt.Errorf("TLS requires CA, certificate and key")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return cert, nil, err
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return cert, nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return cert, nil, fmt.Errorf("no CA certificates found")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return cert, nil, err
	}
	intermediates := x509.NewCertPool()
	for _, der := range cert.Certificate[1:] {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			return cert, nil, err
		}
		intermediates.AddCert(c)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return cert, nil, err
	}
	cert.Leaf = leaf
	return cert, roots, nil
}

// verifyNodeCertificate checks that a node certificate names this node and
// may also serve.
func verifyNodeCertificate(leaf *x509.Certificate, nodeID string) error {
	if certificateIdentity([]*x509.Certificate{leaf}) != domain.NodeIdentity(nodeID) {
		return fmt.Errorf("node certificate identity mismatch")
	}
	if err := leaf.VerifyHostname(nodeID); err != nil {
		return err
	}
	for _, usage := range leaf.ExtKeyUsage {
		if usage == x509.ExtKeyUsageServerAuth || usage == x509.ExtKeyUsageAny {
			return nil
		}
	}
	return fmt.Errorf("node certificate lacks server authentication usage")
}
