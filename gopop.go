package gopop

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

type QueryReq struct {
	Name  string `json:"name"`
	Query string `json:"query"`
	Args  []any  `json:"args"`
}

type CreateReq struct {
	Name      string `json:"name"`
	Migration string `json:"migration"`
}

type Conn struct {
	Url      string
	Username string
	Password string
	Client   *http.Client
}

func New(username string, password string) *Conn {
	return &Conn{
		Username: username,
		Password: password,
		Client:   new(http.Client),
	}
}

func auth(req *http.Request, c *Conn) {
	credentials := c.Username + ":" + c.Password
	encodedCredentials := base64.StdEncoding.EncodeToString([]byte(credentials))
	req.Header.Set("Authorization", "Basic "+encodedCredentials)
}

func (c *Conn) Create(name string, migrationFile string) (*http.Response, error) {
	buffer, err := os.ReadFile(migrationFile)
	if err != nil {
		return nil, err
	}

	msg := CreateReq{
		Name:      name,
		Migration: string(buffer),
	}

	buffer, err = json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprint(c.Url, "/v1/databases")
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}

	auth(req, c)

	res, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Conn) Get(url string, name string) (*http.Response, error) {
	url = fmt.Sprintf(c.Url, "/v1/databases/?name=%s", name)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	auth(req, c)

	res, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Conn) Drop(name string) (*http.Response, error) {
	url := fmt.Sprintf(c.Url, "/v1/databases/?name=%s", name)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	auth(req, c)
	res, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Conn) Query(name string, query string, args ...any) (*http.Response, error) {
	msg := QueryReq{
		Name:  name,
		Query: query,
		Args:  args,
	}

	buffer, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprint(c.Url, "/v1/databases/query")
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}

	auth(req, c)
	res, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Conn) Exec(name string, query string, args ...any) (*http.Response, error) {
	msg := QueryReq{
		Name:  name,
		Query: query,
		Args:  args,
	}

	buffer, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprint(c.Url, "/v1/databases/exec")
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}

	auth(req, c)
	res, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}
