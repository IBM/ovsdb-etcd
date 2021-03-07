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
	buf.WriteString(`["set",[`)
	if s != nil {
		for _, v := range s {
			x, _ := json.Marshal(v)
			buf.WriteString(fmt.Sprintf(`%s,`, x))
		}
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteString(`]]`)
	return buf.Bytes(), nil
}

func (cmr *CondMonitorParameters) UnmarshalJSON(p []byte) error {
	var tmp []json.RawMessage
	if err := json.Unmarshal(p, &tmp); err != nil {
		return fmt.Errorf("Unmarshal json message: %s", err)
	}
	if err := json.Unmarshal(tmp[0], &cmr.DatabaseName); err != nil {
		return fmt.Errorf("Unmarshal database_name: %s", err)
	}
	if err := json.Unmarshal(tmp[1], &cmr.JsonValue); err != nil {
		return fmt.Errorf("Unmarshal json value: %s", err)
	}
	if err := json.Unmarshal(tmp[2], &cmr.MonitorCondRequests); err != nil {
		return fmt.Errorf("Unmarshal monitor conditions: %s", err)
	}
	if len(tmp) > 4 {
		if err := json.Unmarshal(tmp[3], &cmr.LastTxnID); err != nil {
			return fmt.Errorf("Unmarshal last transaction ID: %s", err)
		}
	}
	return nil
}
