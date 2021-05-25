package ovsjson

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
		return fmt.Errorf("unmarshal json message: %s", err)
	}
	if err := json.Unmarshal(tmp[0], &cmr.DatabaseName); err != nil {
		return fmt.Errorf("unmarshal database_name: %s", err)
	}
	l := len(tmp)
	if l < 2 || l > 4 {
		return fmt.Errorf("wrong monitor conditions lenght: %d", l)
	}
	if err := json.Unmarshal(tmp[1], &cmr.JsonValue); err != nil {
		return fmt.Errorf("unmarshal json value: %s", err)
	}
	if err := json.Unmarshal(tmp[2], &cmr.MonitorCondRequests); err != nil {
		obj := map[string]MonitorCondRequest{}
		if err := json.Unmarshal(tmp[2], &obj); err != nil {
			return fmt.Errorf("unmarshal json value into interfaces map: %s", err)
		}
		cmr.MonitorCondRequests = map[string][]MonitorCondRequest{}
		for k, v := range obj {
			cmr.MonitorCondRequests[k] = []MonitorCondRequest{v}
		}
	}
	if l == 4 {
		lastTxnID := ""
		if err := json.Unmarshal(tmp[3], &lastTxnID); err != nil {
			return fmt.Errorf("Unmarshal last transaction ID: %s", err)
		}
		cmr.LastTxnID = &lastTxnID
	}
	return nil
}

func (un UpdateNotification) MarshalJSON() ([]byte, error) {
	var oSet []interface{}
	oSet = append(oSet, un.JasonValue)
	oSet = append(oSet, un.TableUpdates)
	if un.Uuid != nil {
		oSet = append(oSet, un.Uuid.GoUUID)
	}
	return json.Marshal(oSet)
}

func (ru RowUpdate) MarshalJSON() ([]byte, error) {
	obj := map[string]interface{}{}
	if ru.New != nil {
		obj["new"] = *ru.New
	}
	if ru.Old != nil {
		obj["old"] = *ru.Old
	}
	if ru.Initial != nil {
		obj["initial"] = *ru.Initial
	}
	if ru.Modify != nil {
		obj["modify"] = *ru.Modify
	}
	if ru.Insert != nil {
		obj["insert"] = *ru.Insert
	}
	if ru.Delete {
		obj["delete"] = nil
	}
	return json.Marshal(obj)
}

func (ru *RowUpdate) UnmarshalJSON(p []byte) error {
	obj := map[string]interface{}{}
	err := json.Unmarshal(p, &obj)
	if err != nil {
		return err
	}
	v, ok := obj["new"]
	if ok {
		val := v.(map[string]interface{})
		ru.New = &val
	}
	v, ok = obj["old"]
	if ok {
		val := v.(map[string]interface{})
		ru.Old = &val
	}
	v, ok = obj["initial"]
	if ok {
		val := v.(map[string]interface{})
		ru.Initial = &val
	}
	v, ok = obj["insert"]
	if ok {
		val := v.(map[string]interface{})
		ru.Insert = &val
	}
	v, ok = obj["modify"]
	if ok {
		val := v.(map[string]interface{})
		ru.Modify = &val
	}
	_, ok = obj["delete"]
	fmt.Printf("ok = %v\n", ok)
	if ok {
		ru.Delete = true
	}
	return nil
}
