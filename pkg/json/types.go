package json

const ZERO_UUID = "00000000-0000-0000-0000-000000000000"

type Uuid string

type NamedUuid string

type Map map[string]string

type Set []interface{}

type EmptyStruct struct{}

// presents for initial updates
type Initial struct {
	Initial interface{} `json:"initial"`
}

// presents for insert updates
type Insert struct {
	Insert interface{} `json:"insert"`
}

// presents for delete updates
type Delete struct {
	Delete interface{} `json:"delete"`
}

// presents for modify updates
type Modify struct {
	Modify interface{} `json:"modify"`
}
