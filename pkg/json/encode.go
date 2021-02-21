package json

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func (u Uuid) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{"uuid", string(u)})
}

func (u NamedUuid) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{"named-uuid", string(u)})
}

func (m Map) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(`["map",[`)
	if m != nil {
		for k, v := range m {
			buf.WriteString(fmt.Sprintf(`["%s","%s"],`, k, v))
		}
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteString(`]]`)
	return buf.Bytes(), nil
}

func (s Set) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	var needWrap = (len(s) != 1)
	if(needWrap){
		buf.WriteString(`["set",[`)
	}
	if s != nil {
		for _, v := range s {
			x, _ := json.Marshal(v)
			buf.WriteString(fmt.Sprintf(`%s,`, x))
		}
		buf.Truncate(buf.Len() - 1)
	}
	if(needWrap) {
		buf.WriteString(`]]`)
	}
	return buf.Bytes(), nil
}
