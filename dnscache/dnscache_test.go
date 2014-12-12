package dnscache

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

type MockDialer struct {
	mock.Mock
}

func (d *MockDialer) Dial(network, addr string) (net.Conn, error) {
	args := d.Mock.Called(network, addr)
	return args.Get(0).(net.Conn), args.Error(1)
}

type MockConn struct {
	mock.Mock
}

func (c *MockConn) Read(b []byte) (n int, err error) {
	args := c.Mock.Called(b)
	return args.Int(0), args.Error(1)
}

func (c *MockConn) Write(b []byte) (n int, err error) {
	args := c.Mock.Called(b)
	return args.Int(0), args.Error(1)
}

func (c *MockConn) Close() error {
	args := c.Mock.Called()
	return args.Error(0)
}

func (c *MockConn) LocalAddr() net.Addr {
	args := c.Mock.Called()
	return args.Get(0).(net.Addr)
}

func (c *MockConn) RemoteAddr() net.Addr {
	args := c.Mock.Called()
	return args.Get(0).(net.Addr)
}

func (c *MockConn) SetDeadline(t time.Time) error {
	args := c.Mock.Called(t)
	return args.Error(0)
}

func (c *MockConn) SetReadDeadline(t time.Time) error {
	args := c.Mock.Called(t)
	return args.Error(0)
}

func (c *MockConn) SetWriteDeadline(t time.Time) error {
	args := c.Mock.Called(t)
	return args.Error(0)
}

type MockAddr struct {
	mock.Mock
}

func (a *MockAddr) Network() string {
	args := a.Mock.Called()
	return args.String(0)
}

func (a *MockAddr) String() string {
	args := a.Mock.Called()
	return args.String(0)
}

func TestHostnameCached(t *testing.T) {
	addr := &MockAddr{}
	addr.On("String").Return("1.2.3.4")

	conn := &MockConn{}
	conn.On("RemoteAddr").Return(addr)

	dialer := &MockDialer{}
	dialer.On("Dial", "tcp", "test.com").Return(conn, nil).Once()
	dialer.On("Dial", "tcp", "1.2.3.4").Return(conn, nil).Twice()

	cdial, err := Dial(dialer.Dial, 2)
	if err != nil {
		panic(err)
	}
	cdial("tcp", "test.com")
	cdial("tcp", "test.com")
	cdial("tcp", "test.com")
}

func TestHostPushedOutOfCache(t *testing.T) {
	addr := &MockAddr{}
	addr.On("String").Return("1.2.3.4")

	conn := &MockConn{}
	conn.On("RemoteAddr").Return(addr)

	dialer := &MockDialer{}
	dialer.On("Dial", "tcp", "host1.com").Return(conn, nil).Twice()
	dialer.On("Dial", "tcp", "host2.com").Return(conn, nil).Once()
	dialer.On("Dial", "tcp", "host3.com").Return(conn, nil).Once()

	cdial, err := Dial(dialer.Dial, 2)
	if err != nil {
		panic(err)
	}
	cdial("tcp", "host1.com")
	cdial("tcp", "host2.com")
	cdial("tcp", "host3.com")
	cdial("tcp", "host1.com")
}
