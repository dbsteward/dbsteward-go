package lib

// TODO(go,3) no globals
var GlobalSlonik *Slonik = NewSlonik()

type Slonik struct{}

func NewSlonik() *Slonik {
	return &Slonik{}
}

func (self *Slonik) Convert(file string) string {
	// TODO(go,core)
	return ""
}
