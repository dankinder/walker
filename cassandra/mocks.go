package cassandra

import "github.com/iParadigms/walker"

// MockModelDatastore implements walker/cassandra's ModelDatastore interface
// for testing.
type MockModelDatastore struct {
	walker.MockDatastore
}

func (ds *MockModelDatastore) FindLink(u *walker.URL, collectContent bool) (*LinkInfo, error) {
	args := ds.Mock.Called(u, collectContent)
	return args.Get(0).(*LinkInfo), args.Error(1)
}

func (ds *MockModelDatastore) ListLinkHistorical(u *walker.URL) ([]*LinkInfo, error) {
	args := ds.Mock.Called(u)
	return args.Get(0).([]*LinkInfo), args.Error(1)
}

func (ds *MockModelDatastore) InsertLink(link string, excludeDomainReason string) error {
	args := ds.Mock.Called(link, excludeDomainReason)
	return args.Error(0)
}

func (ds *MockModelDatastore) ListLinks(domain string, query LQ) ([]*LinkInfo, error) {
	args := ds.Mock.Called(domain, query)
	return args.Get(0).([]*LinkInfo), args.Error(1)
}

func (ds *MockModelDatastore) InsertLinks(links []string, excludeDomainReason string) []error {
	args := ds.Mock.Called(links, excludeDomainReason)
	return args.Get(0).([]error)
}

func (ds *MockModelDatastore) FindDomain(domain string) (*DomainInfo, error) {
	args := ds.Mock.Called(domain)
	return args.Get(0).(*DomainInfo), args.Error(1)
}

func (ds *MockModelDatastore) ListDomains(query DQ) ([]*DomainInfo, error) {
	args := ds.Mock.Called(query)
	return args.Get(0).([]*DomainInfo), args.Error(1)
}

func (ds *MockModelDatastore) UpdateDomain(domain string, info *DomainInfo, cfg DomainInfoUpdateConfig) error {
	args := ds.Mock.Called(domain, info, cfg)
	return args.Error(0)
}
