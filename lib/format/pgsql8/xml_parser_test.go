package pgsql8_test

import (
	"fmt"
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/stretchr/testify/assert"
)

func TestXmlParser_Process(t *testing.T) {
	// NOTE: this is just the third test case from v1 tests/pgsql8/Pgsql8XmlParserTest.php testProcess
	// the first two are subsets of functionality, so we won't get much out of them

	doc := &ir.Definition{
		Schemas: []*ir.Schema{
			&ir.Schema{
				Name:  "test_schema",
				Owner: "ROLE_OWNER",
				Tables: []*ir.Table{
					&ir.Table{
						Name:       "test_table",
						PrimaryKey: []string{"primary_id"},
						Owner:      "ROLE_OWNER",
						Partitioning: &ir.TablePartition{
							Type: ir.TablePartitionTypeModulo,
							Options: []*ir.TablePartitionOption{
								{Name: "number", Value: "8"},
								{Name: "column", Value: "partition_id"},
							},
						},
						Columns: []*ir.Column{
							{Name: "primary_id", Type: "serial", Nullable: true},
							{Name: "partition_id", Type: "bigint", Nullable: false},
						},
						Indexes: []*ir.Index{
							&ir.Index{
								Name:  "primary_id_idx",
								Using: ir.IndexTypeBtree,
								Dimensions: []*ir.IndexDim{
									{Name: "primary_id", Value: "primary_id"},
								},
								Conditions: []*ir.IndexCond{
									{SqlFormat: ir.SqlFormatPgsql8, Condition: "primary_id IS NOT NULL"},
								},
							},
						},
					},
				},
			},
		},
	}

	expected := &ir.Definition{
		Schemas: []*ir.Schema{
			&ir.Schema{
				Name:  "test_schema",
				Owner: "ROLE_OWNER",
				Tables: []*ir.Table{
					&ir.Table{
						Name:       "test_table",
						PrimaryKey: []string{"primary_id"},
						Owner:      "ROLE_OWNER",
						Partitioning: &ir.TablePartition{
							Type: ir.TablePartitionTypeModulo,
							Options: []*ir.TablePartitionOption{
								{Name: "number", Value: "8"},
								{Name: "column", Value: "partition_id"},
							},
						},
						Columns: []*ir.Column{
							{Name: "primary_id", Type: "serial", Nullable: true},
							{Name: "partition_id", Type: "bigint", Nullable: false},
						},
					},
				},
				Triggers: []*ir.Trigger{
					&ir.Trigger{
						Name:      "test_table_part_trg",
						SqlFormat: ir.SqlFormatPgsql8,
						Events:    []string{"INSERT"},
						Timing:    ir.TriggerTimingBefore,
						ForEach:   ir.TriggerForEachRow,
						Table:     "test_table",
						Function:  "_p_test_schema_test_table.insert_trigger()",
					},
				},
			},
			&ir.Schema{
				Name:   "_p_test_schema_test_table",
				Tables: make([]*ir.Table, 8), // initialized below
				Functions: []*ir.Function{
					&ir.Function{
						Name:        "insert_trigger",
						Returns:     "TRIGGER",
						Owner:       "ROLE_OWNER",
						Description: "DBSteward auto-generated for table partition of test_schema.test_table",
						Definitions: []*ir.FunctionDefinition{
							&ir.FunctionDefinition{
								Language:  "plpgsql",
								SqlFormat: ir.SqlFormatPgsql8,
								Text: `DECLARE
	mod_result INT;
BEGIN
	mod_result := NEW.partition_id % 8;
	IF (mod_result = 0) THEN
		INSERT INTO _p_test_schema_test_table.partition_0 VALUES (NEW.*);
	ELSEIF (mod_result = 1) THEN
		INSERT INTO _p_test_schema_test_table.partition_1 VALUES (NEW.*);
	ELSEIF (mod_result = 2) THEN
		INSERT INTO _p_test_schema_test_table.partition_2 VALUES (NEW.*);
	ELSEIF (mod_result = 3) THEN
		INSERT INTO _p_test_schema_test_table.partition_3 VALUES (NEW.*);
	ELSEIF (mod_result = 4) THEN
		INSERT INTO _p_test_schema_test_table.partition_4 VALUES (NEW.*);
	ELSEIF (mod_result = 5) THEN
		INSERT INTO _p_test_schema_test_table.partition_5 VALUES (NEW.*);
	ELSEIF (mod_result = 6) THEN
		INSERT INTO _p_test_schema_test_table.partition_6 VALUES (NEW.*);
	ELSEIF (mod_result = 7) THEN
		INSERT INTO _p_test_schema_test_table.partition_7 VALUES (NEW.*);
	END IF;
	RETURN NULL;
END;`,
							},
						},
						Grants: []*ir.Grant{
							{Roles: []string{"ROLE_APPLICATION"}, Permissions: []string{"EXECUTE"}},
						},
					},
				},
			},
		},
	}

	// there should be one child table for each parition, with a check constraint
	for i := 0; i < 8; i++ {
		indexCopy := *doc.Schemas[0].Tables[0].Indexes[0]
		child := &ir.Table{
			Name:           fmt.Sprintf("partition_%d", i),
			Owner:          "ROLE_OWNER",
			PrimaryKey:     []string{"primary_id"},
			InheritsSchema: "test_schema",
			InheritsTable:  "test_table",
			Constraints: []*ir.Constraint{
				&ir.Constraint{
					Type:       ir.ConstraintTypeCheck,
					Name:       fmt.Sprintf("test_table_p_%d_chk", i),
					Definition: fmt.Sprintf(`((partition_id %% 8) = %d)`, i),
				},
			},
			// indexes are copied from the parent and renamed
			Indexes: []*ir.Index{
				&indexCopy,
			},
		}
		child.Indexes[0].Name = fmt.Sprintf("%s_p%d", child.Indexes[0].Name, i)
		child.Indexes[0].Dimensions[0].Name = "primary_id_1"

		expected.Schemas[1].Tables[i] = child
	}

	// note that Process mutates the document in place
	err := pgsql8.GlobalXmlParser.Process(doc)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, doc)
}
