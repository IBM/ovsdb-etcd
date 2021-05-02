package libovsdb

import (
	"fmt"
	"reflect"
	"testing"

	"encoding/json"
)

func TestSchema(t *testing.T) {
	type schemaTest struct {
		name           string
		schema         []byte
		expectedErr    bool
		expectedSchema DatabaseSchema
	}

	schemaTestSuite := []schemaTest{
		{
			name: "Simple AtomicType columns",
			schema: []byte(`
		 {"name": "AtomicDB",
		  "version": "0.0.0",
		  "tables": {
		    "atomicTable": {
		      "columns": {
		        "str": {
			  "type": "string"
			},
		        "int": {
			  "type": "integer"
			},
		        "float": {
			  "type": "real"
			},
		        "uuid": {
			  "type": "uuid"
			}
		      }
		    }
		  }
	         }`),
			expectedErr: false,
			expectedSchema: DatabaseSchema{
				Name:    "AtomicDB",
				Version: "0.0.0",
				Tables: map[string]TableSchema{
					"atomicTable": {
						Columns: map[string]*ColumnSchema{
							"str": {
								Type: TypeString,
							},
							"int": {
								Type: TypeInteger,
							},
							"float": {
								Type: TypeReal,
							},
							"uuid": {
								Type: TypeUUID,
							},
						},
					},
				},
			},
		},
		{
			name: "Sets",
			schema: []byte(`
		 {"name": "SetsDB",
		  "version": "0.0.0",
		  "tables": {
		    "setTable": {
		      "columns": {
		        "single": {
			  "type": {
			    "key": "string",
			    "max": 1,
			    "min": 1
			  }
			},
		        "oneElem": {
			  "type": {
			    "key": "uuid",
			    "max": 1,
			    "min": 0
			  }
			},
		        "multipleElem": {
			  "type": {
			    "key": "real",
			    "max": 2,
			    "min": 0
			  }
			},
		        "unlimitedElem": {
			  "type": {
			    "key": "integer",
			    "max": "unlimited",
			    "min": 0
			  }
			},
		        "enumSet": {
			  "type": {
			    "key": {
			      "type": "string",
			      "enum": ["set", ["one", "two"]]
			     },
			    "max": "unlimited",
			    "min": 0
			  }
			}
		      }
		    }
		  }
	         }`),
			expectedErr: false,
			expectedSchema: DatabaseSchema{
				Name:    "SetsDB",
				Version: "0.0.0",
				Tables: map[string]TableSchema{
					"setTable": {
						Columns: map[string]*ColumnSchema{
							"single": {
								Type: TypeString,
								TypeObj: &ColumnType{
									Key: &BaseType{Type: "string"},
									Max: 1,
									Min: 1,
								},
							},
							"oneElem": {
								Type: TypeSet,
								TypeObj: &ColumnType{
									Key: &BaseType{Type: "uuid"},
									Max: 1,
									Min: 0,
								},
							},
							"multipleElem": {
								Type: TypeSet,
								TypeObj: &ColumnType{
									Key: &BaseType{Type: "real"},
									Max: 2,
									Min: 0,
								},
							},
							"unlimitedElem": {
								Type: TypeSet,
								TypeObj: &ColumnType{
									Key: &BaseType{Type: "integer"},
									Max: Unlimited,
									Min: 0,
								},
							},
							"enumSet": {
								Type: TypeSet,
								TypeObj: &ColumnType{
									Key: &BaseType{
										Type: "string",
										Enum: []interface{}{"one", "two"},
									},
									Max: Unlimited,
									Min: 0,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Maps",
			schema: []byte(`
		 {"name": "MapsDB",
		  "version": "0.0.0",
		  "tables": {
		    "mapTable": {
		      "columns": {
		        "str_str": {
			  "type": {
			    "key": "string",
			    "value": "string"
			  }
			},
		        "str_int": {
			  "type": {
			    "key": "string",
			    "value": "integer"
			  }
			},
		        "int_real": {
			  "type": {
			    "key": "integer",
			    "value": "real"
			  }
			},
		        "str_uuid": {
			  "type": {
			    "key": "string",
			    "value": "uuid"
			  }
			},
		        "str_enum": {
			  "type": {
			    "key": "string",
			    "value": {
			      "type": "string",
			      "enum": ["set", ["one", "two"]]
			     }
			  }
			}
		      }
		    }
		  }
	         }`),
			expectedErr: false,
			expectedSchema: DatabaseSchema{
				Name:    "MapsDB",
				Version: "0.0.0",
				Tables: map[string]TableSchema{
					"mapTable": {
						Columns: map[string]*ColumnSchema{
							"str_str": {
								Type: TypeMap,
								TypeObj: &ColumnType{
									Key:   &BaseType{Type: "string"},
									Value: &BaseType{Type: "string"},
									Min:   1,
									Max:   1,
								},
							},
							"str_int": {
								Type: TypeMap,
								TypeObj: &ColumnType{
									Key:   &BaseType{Type: "string"},
									Value: &BaseType{Type: "integer"},
									Min:   1,
									Max:   1,
								},
							},
							"int_real": {
								Type: TypeMap,
								TypeObj: &ColumnType{
									Key:   &BaseType{Type: "integer"},
									Value: &BaseType{Type: "real"},
									Min:   1,
									Max:   1,
								},
							},
							"str_uuid": {
								Type: TypeMap,
								TypeObj: &ColumnType{
									Key:   &BaseType{Type: "string"},
									Value: &BaseType{Type: "uuid"},
									Min:   1,
									Max:   1,
								},
							},
							"str_enum": {
								Type: TypeMap,
								TypeObj: &ColumnType{
									Key: &BaseType{
										Type: "string",
									},
									Value: &BaseType{
										Type: "string",
										Enum: []interface{}{"one", "two"},
									},
									Min: 1,
									Max: 1,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Invalid type",
			schema: []byte(`
		 {"name": "ErrorDB",
		  "version": "0.0.0",
		  "tables": {
		    "errorsTable": {
		      "columns": {
		        "wrongType": {
			  "type": {
			    "key": "uknown"
			  }
			}
		      }
		    }
		  }
	         }`),
			expectedErr: true,
		},
		{
			name:        "Invalid json",
			schema:      []byte(`invalid json`),
			expectedErr: true,
		},
	}

	for _, test := range schemaTestSuite {
		t.Run(fmt.Sprintf("Schema Test %s", test.name), func(t *testing.T) {
			var schema DatabaseSchema
			err := json.Unmarshal(test.schema, &schema)

			if (err != nil) != test.expectedErr {
				t.Fatalf("Expected error to be %t, but got error: %s", test.expectedErr, err.Error())
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(test.expectedSchema, schema) {
				t.Errorf("Expected schema to be %+#v, but got: %+#v", test.expectedSchema, schema)
				// Struct Instrospection for debugging purpuses
				for tname, table := range schema.Tables {
					for n, c := range table.Columns {
						ec := test.expectedSchema.Tables[tname].Columns[n]
						t.Logf("column name %s", n)
						t.Logf("  Expected: %+#v", ec)
						t.Logf("       Got: %+#v", c)

						if ec.TypeObj != nil {
							t.Logf("  Expected.Obj: %+#v", ec.TypeObj)
							if ec.TypeObj.Key != nil {
								t.Logf("    Expected.Obj.Key: %+#v", ec.TypeObj.Key)
							}
							if ec.TypeObj.Value != nil {
								t.Logf("    Expected.Obj.Value: %+#v", ec.TypeObj.Value)
							}
						}
						if c.TypeObj != nil {
							t.Logf("       Got.Obj: %+#v", c.TypeObj)
							if c.TypeObj.Key != nil {
								t.Logf("         Got.Obj.Key: %+#v", c.TypeObj.Key)
							}
							if c.TypeObj.Value != nil {
								t.Logf("         Got.Obj.Value: %+#v", c.TypeObj.Value)
							}
						}

					}
				}
			}
		})
	}

}
