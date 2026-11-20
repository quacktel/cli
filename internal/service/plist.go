package service

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
)

// DefaultLabel is the launchd label of the real agent.
const DefaultLabel = "dev.quacktel.serve"

// Plist is the subset of a launchd job definition that quacktel writes.
type Plist struct {
	Label            string
	ProgramArguments []string
	// LogPath receives launchd's own stdout and stderr for the job.
	LogPath string
}

type node struct {
	XMLName  xml.Name
	Text     string `xml:",chardata"`
	Children []node `xml:",any"`
}

func el(name, text string) node { return node{XMLName: xml.Name{Local: name}, Text: text} }

func dict(children ...node) node {
	return node{XMLName: xml.Name{Local: "dict"}, Children: children}
}

func array(items []string) node {
	n := node{XMLName: xml.Name{Local: "array"}}
	for _, s := range items {
		n.Children = append(n.Children, el("string", s))
	}

	return n
}

const plistHeader = xml.Header + `<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n"

// Render encodes p through encoding/xml so no argument can break out of its element.
func Render(p Plist) ([]byte, error) {
	body := dict(
		el("key", "Label"), el("string", p.Label),
		el("key", "ProgramArguments"), array(p.ProgramArguments),
		el("key", "RunAtLoad"), el("true", ""),
		el("key", "KeepAlive"), dict(el("key", "SuccessfulExit"), el("false", "")),
		el("key", "ThrottleInterval"), el("integer", "30"),
		el("key", "ProcessType"), el("string", "Background"),
		el("key", "StandardOutPath"), el("string", p.LogPath),
		el("key", "StandardErrorPath"), el("string", p.LogPath),
	)

	var buf bytes.Buffer
	buf.WriteString(plistHeader)
	enc := xml.NewEncoder(&buf)
	enc.Indent("", "\t")
	if err := enc.Encode(struct {
		XMLName xml.Name `xml:"plist"`
		Version string   `xml:"version,attr"`
		Dict    node     `xml:"dict"`
	}{Version: "1.0", Dict: body}); err != nil {
		return nil, fmt.Errorf("failed to encode plist: %w", err)
	}
	buf.WriteByte('\n')

	return buf.Bytes(), nil
}

// ReadPlist parses a plist written by Render.
func ReadPlist(path string) (Plist, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is the manager's own plist location
	if err != nil {
		return Plist{}, fmt.Errorf("failed to read %s: %w", path, err)
	}
	var doc struct {
		Dict node `xml:"dict"`
	}
	if err := xml.Unmarshal(data, &doc); err != nil {
		return Plist{}, fmt.Errorf("failed to parse %s: %w", path, err)
	}

	var p Plist
	kids := doc.Dict.Children
	for i := 0; i+1 < len(kids); i += 2 {
		val := kids[i+1]
		switch kids[i].Text {
		case "Label":
			p.Label = val.Text
		case "StandardOutPath":
			p.LogPath = val.Text
		case "ProgramArguments":
			for _, c := range val.Children {
				p.ProgramArguments = append(p.ProgramArguments, c.Text)
			}
		}
	}
	if p.Label == "" {
		return Plist{}, errors.New("plist " + path + " has no Label")
	}

	return p, nil
}
