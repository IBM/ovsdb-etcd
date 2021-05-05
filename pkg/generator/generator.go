package generator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"strings"

	"k8s.io/klog"
)

var (
	SchemaFile     string
	PkgName        string
	BasePackage    string
	OutputFile     string
	DestinationDir string
)

func Run() {
	klog.Infof("Code generating for schema %s", SchemaFile)
	if len(BasePackage) == 0 {
		BasePackage = getBasePackage()
	}
	klog.V(5).Infof("BasePackage = %s", BasePackage)

	data, err := ioutil.ReadFile(SchemaFile)
	if err != nil {
		klog.Errorf("Cannot read schema file %s: %v", SchemaFile, err)
		return
	}
	schemaInst := map[string]interface{}{}
	err = json.Unmarshal(data, &schemaInst)
	if err != nil {
		klog.Errorf("Cannot unmarshal schema file %s: %v", SchemaFile, err)
		return
	}
	if len(PkgName) == 0 {
		PkgName = schemaInst["name"].(string)
	}
	// TODO add configuration
	dir := DestinationDir + "/" + PkgName
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		klog.Errorf("Cannot create output dir %s: %v", dir, err)
		return
	}
	output, err := os.Create(dir + "/" + OutputFile)
	if err != nil {
		klog.Errorf("Cannot create output file %s: %v", dir+"/"+OutputFile, err)
		return
	}
	defer output.Close()
	writer := bufio.NewWriter(output)

	tables, ok := schemaInst["tables"]
	if !ok {
		klog.Warningf("Schema %s doesn't contain tables", SchemaFile)
		return
	}
	tabMaps := map[string][]string{}

	tablesMap := tables.(map[string]interface{})
	keys := make([]string, 0, len(tablesMap))
	for k := range tablesMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, tableName := range keys {
		table := tablesMap[tableName]
		klog.V(6).Infof("Table name %s", tableName)
		tabMap := table.(map[string]interface{})
		columns, ok := tabMap["columns"]
		if !ok {
			klog.Warningf("The table %s doesn't have columns definitions", tableName)
			continue
		}
		columnsTypes, err := parseColumns(tableName, columns)
		if err != nil {
			klog.Warningf("parseColumns for %s returned %v", tableName, err)
			return
		}
		klog.V(6).Infof("Table name %s columns %v", tableName, columnsTypes)
		tabMaps[tableName] = columnsTypes
	}
	err = writeToFile(writer, tabMaps)
	if err != nil {
		klog.Errorf("writeToFile returned %v", err)
		return
	}
	writer.Flush()
	klog.Infof("The new code is stored in %s", dir+"/"+OutputFile)
}

func printStruct(w io.Writer, tableName string, columns []string) error {
	if _, err := fmt.Fprintf(w, "type %s struct { \n", tableName); err != nil {
		return err
	}
	for _, line := range columns {
		if _, err := fmt.Fprintf(w, "\t%s", line); err != nil {
			return err
		}
	}
	// Add UUID and _version
	if _, err := fmt.Fprintf(w, "\t Version libovsdb.UUID `json:\"_version,omitempty\"`\n"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "\t Uuid libovsdb.UUID `json:\"_uuid,omitempty\"`"); err != nil {
		return err
	}

	_, err := fmt.Fprint(w, "}\n\n")
	return err
}

func writeToFile(w io.Writer, structs map[string][]string) error {
	if len(structs) == 0 {
		klog.Warningf("No tables, nothing to write")
		return nil
	}
	if _, err := fmt.Fprintf(w, "package %s\n\n", PkgName); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "import \"%s\"\n\n", BasePackage); err != nil {
		return nil
	}
	keys := make([]string, 0, len(structs))
	for k := range structs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, tableName := range keys {
		columns := structs[tableName]
		if err := printStruct(w, tableName, columns); err != nil {
			return err
		}

	}
	return nil
}

func getValueType(value interface{}) (string, error) {
	switch value.(type) {
	case string:
		return value.(string), nil
	case map[string]interface{}:
		m := value.(map[string]interface{})
		s, ok := m["type"]
		if ok {
			vType, ok := s.(string)
			if !ok {
				klog.Errorf("Value type [%v] is not a string, it's %T", s, s)
				return "", fmt.Errorf("Wrong type for %v", value)
			}
			return vType, nil
		}
		return "", fmt.Errorf("Value %v doesn't have type", value)
	default:
		return "", fmt.Errorf("getValueType, unsupported value type %T %v", value, value)
	}
}

func parseColumns(tableName string, columns interface{}) ([]string, error) {
	columnsMap := columns.(map[string]interface{})
	structColumns := []string{}
	keys := make([]string, 0, len(columnsMap))
	for k := range columnsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, field := range keys {
		v := columnsMap[field]
		fieldName := toUppercase(field)
		typeValue := v.(map[string]interface{})
		var l string
		if vt, ok := typeValue["type"]; ok {
			switch vt.(type) {
			case string:
				// simple case like "name": {"type": "string"}
				l = fmt.Sprintf(" %s\t%s `json:\"%s,omitempty\"`\n", fieldName, typeConvert(vt), field)
			case map[string]interface{}:
				k2Map := vt.(map[string]interface{})
				// check if it's a Set
				max, isMax := k2Map["max"]
				// check if it is a map
				t2v, isMap := k2Map["value"]
				var mapValue string
				if isMap {
					v, err := getValueType(t2v)
					if err != nil {
						return nil, fmt.Errorf("parseColumns, table %s column %s, getValueType returned %v",
							tableName, field, err)
					}
					mapValue = v
				}
				t2, ok := k2Map["key"]
				if !ok {
					return nil, fmt.Errorf("parseColumns, table %s column %s, column vale doesn't have type:key %v",
						tableName, field, k2Map)
				}

				switch t2.(type) {
				case map[string]interface{}:
					k3Map := t2.(map[string]interface{})

					type3, ok := k3Map["type"]
					if !ok {
						return nil, fmt.Errorf("parseColumns, table %s column %s, column vale doesn't have type:key:type %v",
							tableName, field, k3Map)
					}
					if isMap {
						l = createMapFiled(fieldName, type3, mapValue, field)
					} else {
						l = createFiledLine(fieldName, type3, field, max, isMax)
					}
				case string:
					if isMap {
						l = createMapFiled(fieldName, t2, mapValue, field)
					} else {
						l = createFiledLine(fieldName, t2, field, max, isMax)
					}
				default:
					return nil, fmt.Errorf("parseColumns table %s column %s, column type:key is %T", tableName, field, t2)
				}

			default:
				return nil, fmt.Errorf("parseColumns table %s column %s, column type is %T", tableName, field, vt)
			}
			structColumns = append(structColumns, l)
		}
	}
	return structColumns, nil
}

func createMapFiled(fieldName string, keyType interface{}, valueType interface{}, field string) string {
	return fmt.Sprintf(" %s \t%s `json:\"%s,omitempty\"`\n", fieldName, "libovsdb.OvsMap", field)
}

func createFiledLine(fieldName string, filedType interface{}, field string, max interface{}, isSet bool) string {
	if isSet {
		return fmt.Sprintf(" %s \t%s `json:\"%s,omitempty\"`\n", fieldName, "libovsdb.OvsSet", field)
	}
	return fmt.Sprintf(" %s \t%s `json:\"%s,omitempty\"`\n", fieldName, typeConvert(filedType), field)
}

func typeConvert(typeName interface{}) string {
	s := fmt.Sprintf("%s", typeName)
	switch s {
	case "string":
		return s
	case "boolean":
		return "bool"
	case "integer":
		return "int64"
	case "real":
		return "float64"
	case "uuid":
		return "libovsdb.UUID"
	}
	// TODO
	return ""
}

func toUppercase(name string) string {
	startLetter := name[:1]
	startLetter = strings.ToUpper(startLetter)
	return startLetter + name[1:]
}

func Package() string {
	pc, _, _, _ := runtime.Caller(1)
	parts := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	pl := len(parts)
	pkage := ""
	funcName := parts[pl-1]
	if parts[pl-2][0] == '(' {
		funcName = parts[pl-2] + "." + funcName
		pkage = strings.Join(parts[0:pl-2], ".")
	} else {
		pkage = strings.Join(parts[0:pl-1], ".")
	}
	return pkage
}

func getBasePackage() string {
	return "github.com/ibm/ovsdb-etcd/pkg/libovsdb"
}
