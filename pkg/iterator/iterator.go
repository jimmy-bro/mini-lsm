package iterator

type Iter interface {
	Key() []byte
	Value() []byte
	IsValid() bool
	Next()
}
