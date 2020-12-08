package model

type Sequence struct {
	Grants []*Grant `xml:"grant"`
}
