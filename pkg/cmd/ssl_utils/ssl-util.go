package ssl_utils

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"reflect"
)

var SslVersion = flag.String("ssl-version", "", "choose ssl version to use")
var SslCipherSuite = flag.String("ssl-cipher-suite", "", "choose ssl cipher suite to use")

const (
	VersionTLS10 = "VersionTLS10"
	VersionTLS11 = "VersionTLS11"
	VersionTLS12 = "VersionTLS12"
	VersionTLS13 = "VersionTLS13"
)

var (
	//supportedUpToTLS12 = []uint16{VersionTLS10, VersionTLS11, VersionTLS12}
	//supportedOnlyTLS12 = []uint16{VersionTLS12}
	supportedOnlyTLS13 = []uint16{tls.VersionTLS13}
)

type CipherSuite tls.CipherSuite

type Version struct {
	Name string
	ID   uint16
}

func (version Version) String() string {
	return fmt.Sprintf("'%s'", version.Name)
}

func (cipherSuite CipherSuite) String() string {
	return fmt.Sprintf("'%s'", cipherSuite.Name)
}

func AddSslToConfig(conf *tls.Config, sslCipherSuitesName string, sslVersionName string) error {
	if conf == nil {
		return errors.New("conf is an empty pointer")
	}
	var cipherSuite CipherSuite
	var version Version
	var err error
	if sslCipherSuitesName != "" {
		cipherSuite, err = GetCipherSuite(sslCipherSuitesName)
		if err != nil {
			return err
		}
	}
	if sslVersionName != "" {
		version, err = GetVersion(sslVersionName)
		if err != nil {
			return err
		}
	}
	if sslCipherSuitesName != "" && sslVersionName != "" {
		err = assertCipherSuiteSupportsSSLVersion(cipherSuite, version)
		if err != nil {
			return err
		}
	}
	if sslCipherSuitesName != "" {
		conf.CipherSuites = []uint16{cipherSuite.ID}
	}
	if sslVersionName != "" {
		conf.MaxVersion = version.ID
		conf.MinVersion = version.ID
	}
	return nil
}

func assertCipherSuiteSupportsSSLVersion(cipherSuite CipherSuite, version Version) error {
	if cipherSuiteSupportsVersion(cipherSuite, version) {
		return nil
	}
	return errors.New(fmt.Sprintf("cipher suite %s not support ssl version %s. available ssl versions for this cipher suite:%s", cipherSuite, version, getVersionsSupportedByCipherSuite(cipherSuite)))
}

func getSupportedCipherSuites() []CipherSuite {
	var res []CipherSuite
	for _, suit := range tls.CipherSuites() {
		res = append(res, CipherSuite(*suit))
	}
	return res
}

func getSupportedVersions() []Version {
	return []Version{
		{VersionTLS10, tls.VersionTLS10},
		{VersionTLS11, tls.VersionTLS11},
		{VersionTLS12, tls.VersionTLS12},
		{VersionTLS13, tls.VersionTLS13},
	}
}

func GetCipherSuite(name string) (CipherSuite, error) {
	for _, suite := range getSupportedCipherSuites() {
		if suite.Name == name {
			return suite, nil
		}
	}
	return CipherSuite{}, errors.New(fmt.Sprintf("not supported cipher suite. available cipher suites:%s", getSupportedCipherSuites()))
}

func GetVersion(condition interface{}) (Version, error) {
	for _, version := range getSupportedVersions() {
		switch condition := condition.(type) {
		case string:
			if version.Name == condition {
				return version, nil
			}
		case uint16:
			if version.ID == condition {
				return version, nil
			}
		default:
			return Version{}, errors.New(fmt.Sprintf("not supported condition type for GetVersion %T", condition))
		}
	}
	return Version{}, errors.New(fmt.Sprintf("not supported ssl version. available ssl versions:%s", getSupportedVersions()))
}

func getCipherSuitesFilteredByCond(cond func(CipherSuite) bool) []CipherSuite {
	var res []CipherSuite
	for _, suite := range getSupportedCipherSuites() {
		if cond(suite) {
			res = append(res, suite)
		}
	}
	return res
}

func GetCipherSuitesSupportedOnlyTLS3() []CipherSuite {
	return getCipherSuitesFilteredByCond(func(cs CipherSuite) bool { return reflect.DeepEqual(cs.SupportedVersions, supportedOnlyTLS13) })
}

func GetVersionsFilteredByCond(cond func(Version) bool) []Version {
	var res []Version
	for _, version := range getSupportedVersions() {
		if cond(version) {
			res = append(res, version)
		}
	}
	return res
}

func getVersionsSupportedByCipherSuite(cipherSuite CipherSuite) []Version {
	cond := func(version Version) bool {
		return cipherSuiteSupportsVersion(cipherSuite, version)
	}
	return GetVersionsFilteredByCond(cond)
}

func cipherSuiteSupportsVersion(cipherSuite CipherSuite, version Version) bool {
	for _, supportedCiphesuiteVersionID := range cipherSuite.SupportedVersions {
		if supportedCiphesuiteVersionID == version.ID {
			return true
		}
	}
	return false
}
