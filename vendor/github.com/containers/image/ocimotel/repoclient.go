package ocimotel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

type OciRepo struct {
	url string
	ref *ociMotelReference
}

func NewOciRepo(ref *ociMotelReference) (r OciRepo, err error) {
	r = OciRepo{ref: ref}
	server := "127.0.0.1"
	port := "8080"
	if ref.server != "" {
		server = ref.server
	}
	if ref.port != -1 {
		port = fmt.Sprintf("%d", ref.port)
	}
	r.url = fmt.Sprintf("http://%s:%s", server, port)
	queryURI := fmt.Sprintf("%s/v2/", r.url)
	client := &http.Client{}
	resp, err := client.Get(queryURI)
	if err != nil {
		return r, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		return r, nil
	}
	return r, fmt.Errorf("Unexpected return code %d from %s", resp.StatusCode, r.url)
}

func (o *OciRepo) GetManifest() ([]byte, *ispec.Manifest, error) {
	name := o.ref.name
	tag := o.ref.tag
	m := &ispec.Manifest{}
	var body []byte
	uri := fmt.Sprintf("%s/v2/%s/manifests/%s", o.url, name, tag)
	resp, err := http.Get(uri)
	if err != nil {
		return body, m, errors.Wrapf(err, "Error getting manifest %s %s from %s", name, tag, o.url)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return body, m, fmt.Errorf("Bad return code %d getting manifest", resp.StatusCode)
	}
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return body, m, errors.Wrapf(err, "Error reading response body for %s", tag)
	}
	err = json.Unmarshal(body, m)
	if err != nil {
		return body, m, errors.Wrap(err, "Failed decoding response")
	}
	return body, m, nil
}

func (o *OciRepo) RemoveManifest() error {
	name := o.ref.name
	tag := o.ref.tag
	uri := fmt.Sprintf("%s/v2/%s/manifests/%s", o.url, name, tag)
	client := &http.Client{}
	request, err := http.NewRequest("DELETE", uri, nil)
	if err != nil {
		return errors.Wrapf(err, "Couldn't create DELETE request for %s", uri)
	}
	resp, err := client.Do(request)
	if err != nil {
		return errors.Wrapf(err, "Error deleting manifest")
	}
	if resp.StatusCode != 202 {
		return fmt.Errorf("Server returned unexpected code %d", resp.StatusCode)
	}
	return nil
}

func (o *OciRepo) PutManifest(body []byte) error {
	name := o.ref.name
	tag := o.ref.tag
	uri := fmt.Sprintf("%s/v2/%s/manifests/%s", o.url, name, tag)

	client := &http.Client{}
	request, err := http.NewRequest("PUT", uri, bytes.NewReader(body))
	if err != nil {
		return errors.Wrapf(err, "Error creating request for %s", uri)
	}
	resp, err := client.Do(request)
	if err != nil {
		return errors.Wrapf(err, "Error posting manifest")
	}
	if resp.StatusCode != 201 {
		return fmt.Errorf("Server returned unexpected code %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	return nil
}

//HEAD /v2/<name>/blobs/<digest>  -> 200 (has layer)
func (o *OciRepo) HasLayer(ldigest string) bool {
	name := o.ref.name
	uri := fmt.Sprintf("%s/v2/%s/blobs/%s", o.url, name, ldigest)
	client := &http.Client{}
	resp, err := client.Head(uri)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func (o *OciRepo) GetLayer(ldigest string) (io.ReadCloser, int64, error) {
	name := o.ref.name
	uri := fmt.Sprintf("%s/v2/%s/blobs/%s", o.url, name, ldigest)
	resp, err := http.Get(uri)
	if err != nil {
		return nil, -1, errors.Wrapf(err, "Error getting layer %s", ldigest)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, -1, fmt.Errorf("Bad return code %d getting layer", resp.StatusCode)
	}
	return resp.Body, -1, err
}

func (o *OciRepo) PostLayer(ldigest digest.Digest, stream io.Reader) error {
	name := o.ref.name
	uri := fmt.Sprintf("%s/v2/%s/blobs/uploads/?digest=%s", o.url, name, ldigest.Encoded())

	client := &http.Client{}
	req, err := http.NewRequest("POST", uri, stream)
	if err != nil {
		return errors.Wrap(err, "Failed opening POST request")
	}
	req.Header.Add("Host", "localhost")
	// How to get the Content-Length from the stream?  I don't want to
	// process the stream twice;  we could pass in the ocidir string
	// and call fileSize on the blob path, but that sucks too.
	//req.Header.Add("Content-Length", fmt.Sprintf("%d", size))
	req.Header.Add("Content-Type", "binary/octet-stream")
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "Failed posting request %v", req)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return fmt.Errorf("Server returned an error %d", resp.StatusCode)
	}

	return nil
}
