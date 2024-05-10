package lib

type SlonyOperations interface {
	SlonyCompare(file string)
	SlonyDiff(oldFile, newFile string)
}

type Slonik struct{}

func NewSlonik() *Slonik {
	return &Slonik{}
}

func (slonik *Slonik) Convert(file string) string {
	// TODO(go,core)
	return ""
}
